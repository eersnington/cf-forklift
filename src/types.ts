import type {
	WorkflowStep,
	WorkflowStepConfig,
	WorkflowStepContext,
	WorkflowStepRollbackOptions,
} from "cloudflare:workers";

/** @internal Symbol used to attach implementation state to fork objects. */
export const forkState = Symbol("cf-forklift.forkState");

/** Serializable value shape accepted by Cloudflare Workflow step output. */
export type Serializable<T extends Rpc.Serializable<T>> = T;

export type MarkerMode = "off" | "minimal" | "summary";

/** @internal Resolved cooperative abort policy for a join run. */
export type AbortOnFailure = "none" | "cooperative";

/**
 * Configuration for `withWorkflow()`.
 *
 * @example
 * ```ts
 * const workflow = withWorkflow(step, {
 * 	stepNameSeparator: " / ",
 * 	markers: "summary",
 * });
 * ```
 */
export interface WorkflowOptions {
	/**
	 * Separator used when cf-forklift creates Cloudflare step names.
	 *
	 * Branch steps are named as `<fork><separator><branch><separator><step>`.
	 * Marker steps are named as `<fork><separator>fork` and
	 * `<fork><separator>join`.
	 *
	 * @default " / "
	 */
	readonly stepNameSeparator?: string;

	/**
	 * Controls whether cf-forklift emits real Workflow marker steps around forks
	 * and joins.
	 *
	 * `"summary"` records structured marker output, `"minimal"` records
	 * breadcrumb-only marker steps, and `"off"` disables marker steps.
	 *
	 * @default "summary"
	 */
	readonly markers?: MarkerMode;
}

/**
 * Options for `workflow.join.required()`.
 *
 * @example
 * ```ts
 * await workflow.join.required(checks, {
 * 	abortOnFailure: "cooperative",
 * });
 * ```
 */
export interface RequiredJoinOptions {
	/**
	 * Requests cooperative abort after the first branch failure.
	 *
	 * Branches observe the request through `signal` or
	 * `cancellation.throwIfRequested()`. This skips future branch work at
	 * checkpoints, but does not interrupt an already-started Cloudflare Workflow
	 * primitive.
	 */
	readonly abortOnFailure?: "cooperative";
}

/**
 * Reason attached to a branch that stopped after a cooperative abort request.
 *
 * @example
 * ```ts
 * if (outcome.status === "aborted") {
 * 	console.log(outcome.reason.sourceBranchName);
 * }
 * ```
 */
export interface ForkAbortReason {
	/** The kind of event that requested cooperative abort. */
	readonly type: "branch-failure";
	/** The fork whose branch work was aborted. */
	readonly forkName: string;
	/** The branch whose failure requested cooperative abort. */
	readonly sourceBranchName: string;
}

/**
 * Branch-local cooperative abort helper.
 *
 * Use `throwIfRequested()` before starting optional follow-up work after a
 * previous branch step has completed.
 *
 * @example
 * ```ts
 * const fork = workflow.fork("verify merchant", {
 * 	risk: async ({ step, cancellation }) => {
 * 		await step.do("risk check 1", riskCheckOne);
 * 		cancellation.throwIfRequested();
 * 		return await step.do("risk check 2", riskCheckTwo);
 * 	},
 * });
 * ```
 */
export interface ForkCancellation {
	/**
	 * Standard `AbortSignal` for abort-aware user code such as `fetch`.
	 */
	readonly signal: AbortSignal;

	/**
	 * `true` after a required join with cooperative abort has observed a branch
	 * failure and requested sibling branches to stop at checkpoints.
	 */
	readonly requested: boolean;

	/**
	 * Reason for the cooperative abort request, if one has been made.
	 */
	readonly reason: ForkAbortReason | undefined;

	/**
	 * Throws `ForkAbortError` if cooperative abort has been requested.
	 */
	throwIfRequested(): void;
}

/**
 * Outcome for one branch in a settled join or failed required join.
 *
 * @example
 * ```ts
 * const outcomes = await workflow.join.settled(checks);
 *
 * if (outcomes.bank.status === "failure") {
 * 	console.error(outcomes.bank.error);
 * }
 * ```
 */
export type WorkflowOutcome<T> =
	| WorkflowSuccess<T>
	| WorkflowFailure
	| WorkflowAborted;

interface WorkflowSuccess<T> {
	/** Branch completed successfully. */
	readonly status: "success";
	/** Value returned by the branch. */
	readonly value: T;
}

interface WorkflowFailure {
	/** Branch threw a non-abort error. */
	readonly status: "failure";
	/** Original branch error. */
	readonly error: unknown;
}

interface WorkflowAborted {
	/** Branch observed cooperative abort and skipped future work. */
	readonly status: "aborted";
	/** Reason the branch stopped. */
	readonly reason: ForkAbortReason;
}

export interface ScopedWorkflowStep {
	/**
	 * Runs a Cloudflare Workflow step with the current fork and branch name
	 * prefixed onto the step name.
	 *
	 * @example
	 * ```ts
	 * profile: ({ step }) => step.do("verify profile", verifyProfile)
	 * ```
	 */
	do<T extends Rpc.Serializable<T>>(
		name: string,
		callback: (ctx: WorkflowStepContext) => Promise<T>,
		rollbackOptions?: WorkflowStepRollbackOptions<T>
	): Promise<T>;

	/**
	 * Runs a configured Cloudflare Workflow step with the current fork and branch
	 * name prefixed onto the step name.
	 *
	 * @example
	 * ```ts
	 * bank: ({ step }) =>
	 * 	step.do("verify bank", { retries: { limit: 3 } }, verifyBank)
	 * ```
	 */
	do<T extends Rpc.Serializable<T>>(
		name: string,
		config: WorkflowStepConfig,
		callback: (ctx: WorkflowStepContext) => Promise<T>,
		rollbackOptions?: WorkflowStepRollbackOptions<T>
	): Promise<T>;

	/** Runs a branch-scoped Cloudflare `step.sleep()`. */
	sleep: WorkflowStep["sleep"];

	/** Runs a branch-scoped Cloudflare `step.sleepUntil()`. */
	sleepUntil: WorkflowStep["sleepUntil"];

	/** Runs a branch-scoped Cloudflare `step.waitForEvent()`. */
	waitForEvent: WorkflowStep["waitForEvent"];
}

/**
 * Context passed to each fork branch.
 *
 * @example
 * ```ts
 * const checks = workflow.fork("verify merchant", {
 * 	bank: async ({ step, branchName, cancellation }) => {
 * 		await step.do("verify bank", verifyBank);
 * 		cancellation.throwIfRequested();
 * 		return branchName;
 * 	},
 * });
 * ```
 */
export interface BranchContext {
	/** Branch-scoped Workflow step helpers. */
	readonly step: ScopedWorkflowStep;

	/** Name of the fork that owns this branch. */
	readonly forkName: string;

	/** Name of this branch. */
	readonly branchName: string;

	/** Standard abort signal for abort-aware user code. */
	readonly signal: AbortSignal;

	/** Cooperative abort helper for branch checkpoints. */
	readonly cancellation: ForkCancellation;
}

/** Function that runs one fork branch. */
export type BranchFactory<TResult> = (
	context: BranchContext
) => Promise<TResult>;

export type BranchRecord = Record<string, BranchFactory<unknown>>;

export type RequiredJoinResult<TBranches extends BranchRecord> = {
	[K in keyof TBranches]: Awaited<ReturnType<TBranches[K]>>;
};

export type SettledJoinResult<TBranches extends BranchRecord> = {
	[K in keyof TBranches]: WorkflowOutcome<Awaited<ReturnType<TBranches[K]>>>;
};

/** @internal Join policy used when emitting markers and running branch work. */
export type JoinPolicy = "required" | "settled";

/** @internal Structured value returned by summary fork marker steps. */
export interface ForkMarker {
	readonly type: "fork";
	readonly name: string;
	readonly branches: string[];
	readonly policy: JoinPolicy;
	readonly abortOnFailure: AbortOnFailure;
}

/** @internal Structured value returned by summary join marker steps. */
export interface JoinMarker {
	readonly type: "join";
	readonly name: string;
	readonly policy: JoinPolicy;
	readonly abortOnFailure: AbortOnFailure;
	readonly status: "success" | "failure";
	readonly branches: Record<string, WorkflowOutcome<unknown>["status"]>;
}

/** @internal Per-join execution state shared by branch contexts. */
export interface ForkRun {
	readonly forkName: string;
	readonly abortOnFailure: AbortOnFailure;
	readonly controller: AbortController;
	abortReason: ForkAbortReason | undefined;
}

/** @internal Mutable state captured by a lazily constructed fork. */
export interface ForkState {
	readonly name: string;
	readonly rootStep: WorkflowStep;
	readonly options: RequiredWorkflowOptions;
	readonly branches: Map<string, BranchFactory<unknown>>;
}

/** @internal Fully resolved `withWorkflow()` options. */
export interface RequiredWorkflowOptions {
	readonly stepNameSeparator: string;
	readonly markers: MarkerMode;
}

/**
 * A lazily constructed fork.
 *
 * A fork is a named collection of branch functions. Creating a fork only records
 * the branches; no branch work starts until the fork is passed to
 * `workflow.join.required()` or `workflow.join.settled()`.
 *
 * Branch names must be unique within the fork because they become result keys,
 * marker keys, and Cloudflare step-name path segments. For example, branch
 * `bank` with step `verify bank` inside fork `verify merchant` becomes:
 *
 * ```txt
 * verify merchant / bank / verify bank
 * ```
 *
 * Use a fork when several independent pieces of Workflow work can run in
 * parallel and later be joined into one keyed result object.
 *
 * @example
 * Static fork with typed branch results:
 *
 * ```ts
 * const checks = workflow.fork("verify merchant", {
 * 	profile: ({ step }) => step.do("verify profile", verifyProfile),
 * 	bank: ({ step }) => step.do("verify bank", verifyBank),
 * });
 *
 * const result = await workflow.join.required(checks);
 * result.profile;
 * result.bank;
 * ```
 *
 * @example
 * Dynamic fork construction:
 *
 * ```ts
 * const checks = workflow.fork("verify merchant")
 * 	.branch("profile", ({ step }) => step.do("verify profile", verifyProfile))
 * 	.branch("bank", ({ step }) => step.do("verify bank", verifyBank));
 *
 * const result = await workflow.join.required(checks);
 * ```
 */
export interface Fork<TBranches extends BranchRecord> {
	/**
	 * Name used as the fork scope in marker steps and branch step names.
	 */
	readonly name: string;

	/**
	 * Adds a branch to this fork and returns the same fork with widened branch
	 * result types.
 	 *
	 * The branch name must be unique within the fork. Branch names become keys in
	 * required-join results, settled-join outcomes, and marker output.
	 *
	 * @example
	 * ```ts
	 * const checks = workflow.fork("run checks")
	 * 	.branch("bank", ({ step }) => step.do("verify bank", verifyBank))
	 * 	.branch("risk", ({ step }) => step.do("screen risk", screenRisk));
	 * ```
 	 */
	branch<TName extends string, TResult>(
		name: TName,
		factory: BranchFactory<TResult>
	): Fork<TBranches & Record<TName, BranchFactory<TResult>>>;
}

/** @internal Fork object plus hidden implementation state. */
export type InternalFork<TBranches extends BranchRecord> = Fork<TBranches> & {
	readonly [forkState]: ForkState;
};

/**
 * Fork/join helper returned by `withWorkflow()`.
 *
 * Use `fork()` to describe branch work, then use `join.required()` or
 * `join.settled()` to run and collect that work. Fork construction is lazy, so
 * the join method controls when branches start, which marker steps are emitted,
 * and how failures are reported.
 *
 * @example
 * Required join for all-or-nothing branch work:
 *
 * ```ts
 * const workflow = withWorkflow(step);
 * const checks = workflow.fork("verify merchant", {
 * 	profile: ({ step }) => step.do("verify profile", verifyProfile),
 * 	bank: ({ step }) => step.do("verify bank", verifyBank),
 * });
 *
 * const values = await workflow.join.required(checks);
 * ```
 *
 * @example
 * Settled join for best-effort branch work:
 *
 * ```ts
 * const enrichment = workflow.fork("enrich merchant", {
 * 	website: ({ step }) => step.do("check website", checkWebsite),
 * 	marketplace: ({ step }) => step.do("check marketplace", checkMarketplace),
 * });
 *
 * const outcomes = await workflow.join.settled(enrichment);
 * ```
 */
export interface Workflow {
	/**
	 * Creates a fork from a static branch record.
	 *
	 * Prefer this form when branch names are known in code. TypeScript preserves
	 * the branch keys in required-join results and settled-join outcomes.
	 *
	 * @example
	 * ```ts
	 * const checks = workflow.fork("verify merchant", {
	 * 	profile: ({ step }) => step.do("verify profile", verifyProfile),
	 * 	bank: ({ step }) => step.do("verify bank", verifyBank),
	 * });
	 * ```
	 */
	fork<TBranches extends BranchRecord>(
		name: string,
		branches: TBranches
	): Fork<TBranches>;

	/**
	 * Creates an empty fork for dynamic branch construction.
	 *
	 * Use this form when branch names come from runtime data. Branch names still
	 * must be deterministic and unique within the fork.
	 *
	 * Runtime dynamic branches work normally, but TypeScript only preserves exact
	 * result keys when branch names are known as literal types.
	 *
	 * @example
	 * ```ts
	 * const checks = workflow.fork("run checks");
	 *
	 * for (const check of enabledChecks) {
	 * 	checks.branch(check.name, ({ step }) =>
	 * 		step.do(check.stepName, () => runCheck(check))
	 * 	);
	 * }
	 * ```
	 */
	fork(name: string): Fork<Record<never, never>>;

	/**
	 * Join operations that start fork branches and collect their outcomes.
	 */
	readonly join: {
		/**
		 * Starts all branches, waits for every branch to settle, and returns keyed
		 * branch values if every branch succeeds.
		 *
		 * Use this when every branch is required for the Workflow to continue. If any
		 * branch fails or observes cooperative abort, the join throws `ForkJoinError`
		 * after all branches have reached a terminal outcome. The error includes all
		 * branch outcomes.
		 *
		 * @example
		 * ```ts
		 * const values = await workflow.join.required(checks);
		 * ```
		 *
		 * @example
		 * Request cooperative abort after the first branch failure:
		 *
		 * ```ts
		 * await workflow.join.required(checks, {
		 * 	abortOnFailure: "cooperative",
		 * });
		 * ```
		 */
		required<TBranches extends BranchRecord>(
			fork: Fork<TBranches>,
			options?: RequiredJoinOptions
		): Promise<RequiredJoinResult<TBranches>>;

		/**
		 * Starts all branches, waits for every branch to settle, and returns keyed
		 * branch outcomes without throwing because a branch failed.
		 *
		 * Use this when branch failures are data the Workflow can inspect and handle.
		 * Each outcome is keyed by branch name.
		 *
		 * @example
		 * ```ts
		 * const outcomes = await workflow.join.settled(enrichment);
		 *
		 * if (outcomes.website.status === "failure") {
		 * 	// Continue with partial enrichment.
		 * }
		 * ```
		 */
		settled<TBranches extends BranchRecord>(
			fork: Fork<TBranches>
		): Promise<SettledJoinResult<TBranches>>;
	};
}
