import type {
	WorkflowStep,
	WorkflowStepConfig,
	WorkflowStepContext,
	WorkflowStepRollbackOptions,
} from "cloudflare:workers";

const forkState = Symbol("cf-forklift.forkState");

type Serializable<T> = T extends
	| undefined
	| null
	| boolean
	| number
	| bigint
	| string
	| ReadableStream
	| Blob
	? T
	: T extends Array<infer U>
		? Array<Serializable<U>>
		: T extends Map<infer K, infer V>
			? Map<Serializable<K>, Serializable<V>>
			: T extends Set<infer V>
				? Set<Serializable<V>>
				: T extends object
					? {
							[K in keyof T as K extends string | number
								? K
								: never]: Serializable<T[K]>;
						}
					: never;

type MarkerMode = "off" | "minimal" | "summary";
type AbortOnFailure = "none" | "cooperative";

export type WorkflowOptions = {
	readonly stepNameSeparator?: string;
	readonly markers?: MarkerMode;
};

export type RequiredJoinOptions = {
	readonly abortOnFailure?: "cooperative";
};

export type ForkAbortReason = {
	readonly type: "branch-failure";
	readonly forkName: string;
	readonly sourceBranchName: string;
};

export type ForkCancellation = {
	readonly signal: AbortSignal;
	readonly requested: boolean;
	readonly reason: ForkAbortReason | undefined;
	throwIfRequested(): void;
};

export type WorkflowOutcome<T> =
	| { readonly status: "success"; readonly value: T }
	| { readonly status: "failure"; readonly error: unknown }
	| { readonly status: "aborted"; readonly reason: ForkAbortReason };

type ScopedWorkflowStep = {
	do<T>(
		name: string,
		callback: (ctx: WorkflowStepContext) => Promise<T>,
		rollbackOptions?: WorkflowStepRollbackOptions<T>
	): Promise<Serializable<T>>;
	do<T>(
		name: string,
		config: WorkflowStepConfig,
		callback: (ctx: WorkflowStepContext) => Promise<T>,
		rollbackOptions?: WorkflowStepRollbackOptions<T>
	): Promise<Serializable<T>>;
	sleep: WorkflowStep["sleep"];
	sleepUntil: WorkflowStep["sleepUntil"];
	waitForEvent: WorkflowStep["waitForEvent"];
};

type BranchContext = {
	readonly step: ScopedWorkflowStep;
	readonly forkName: string;
	readonly branchName: string;
	readonly signal: AbortSignal;
	readonly cancellation: ForkCancellation;
};

type BranchFactory<TResult> = (
	context: BranchContext
) => Promise<TResult>;

type BranchRecord = Record<string, BranchFactory<unknown>>;

type RequiredJoinResult<TBranches extends BranchRecord> = {
	[K in keyof TBranches]: Awaited<ReturnType<TBranches[K]>>;
};

type SettledJoinResult<TBranches extends BranchRecord> = {
	[K in keyof TBranches]: WorkflowOutcome<Awaited<ReturnType<TBranches[K]>>>;
};

type JoinPolicy = "required" | "settled";

type ForkMarker = {
	readonly type: "fork";
	readonly name: string;
	readonly branches: string[];
	readonly policy: JoinPolicy;
	readonly abortOnFailure: AbortOnFailure;
};

type JoinMarker = {
	readonly type: "join";
	readonly name: string;
	readonly policy: JoinPolicy;
	readonly abortOnFailure: AbortOnFailure;
	readonly status: "success" | "failure";
	readonly branches: Record<string, WorkflowOutcome<unknown>["status"]>;
};

type ForkRun = {
	readonly forkName: string;
	readonly abortOnFailure: AbortOnFailure;
	readonly controller: AbortController;
	abortReason: ForkAbortReason | undefined;
};

type ForkState = {
	readonly name: string;
	readonly rootStep: WorkflowStep;
	readonly options: RequiredWorkflowOptions;
	readonly branches: Map<string, BranchFactory<unknown>>;
};

type RequiredWorkflowOptions = {
	readonly stepNameSeparator: string;
	readonly markers: MarkerMode;
};

export type Fork<TBranches extends BranchRecord> = {
	readonly name: string;
	branch<TName extends string, TResult>(
		name: TName,
		factory: BranchFactory<TResult>
	): Fork<TBranches & Record<TName, BranchFactory<TResult>>>;
};

type InternalFork<TBranches extends BranchRecord> = Fork<TBranches> & {
	readonly [forkState]: ForkState;
};

export type Workflow = {
	fork<TBranches extends BranchRecord>(
		name: string,
		branches: TBranches
	): Fork<TBranches>;
	fork(name: string): Fork<Record<never, never>>;
	readonly join: {
		required<TBranches extends BranchRecord>(
			fork: Fork<TBranches>,
			options?: RequiredJoinOptions
		): Promise<RequiredJoinResult<TBranches>>;
		settled<TBranches extends BranchRecord>(
			fork: Fork<TBranches>
		): Promise<SettledJoinResult<TBranches>>;
	};
};

export class ForkJoinError extends Error {
	readonly forkName: string;
	readonly outcomes: Record<string, WorkflowOutcome<unknown>>;

	constructor(options: {
		readonly forkName: string;
		readonly outcomes: Record<string, WorkflowOutcome<unknown>>;
	}) {
		const failures = Object.entries(options.outcomes)
			.filter(([, outcome]) => outcome.status === "failure")
			.map(([name]) => name);

		super(
			`Fork "${options.forkName}" failed in ${failures.length} branch(es): ${failures.join(
				", "
			)}. Completed branches were drained; inspect outcomes before retrying or compensating.`
		);
		this.name = "ForkJoinError";
		this.forkName = options.forkName;
		this.outcomes = options.outcomes;
	}
}

export class ForkAbortError extends Error {
	readonly forkName: string;
	readonly branchName: string;
	readonly reason: ForkAbortReason;

	constructor(options: {
		readonly forkName: string;
		readonly branchName: string;
		readonly reason: ForkAbortReason;
	}) {
		super(
			`Branch "${options.branchName}" in fork "${options.forkName}" stopped because branch "${options.reason.sourceBranchName}" failed. Completed branch work was preserved; future branch work was skipped cooperatively.`
		);
		this.name = "ForkAbortError";
		this.forkName = options.forkName;
		this.branchName = options.branchName;
		this.reason = options.reason;
	}
}

export function withWorkflow(
	step: WorkflowStep,
	options: WorkflowOptions = {}
): Workflow {
	const resolvedOptions = {
		stepNameSeparator: options.stepNameSeparator ?? " / ",
		markers: options.markers ?? "summary",
	} satisfies RequiredWorkflowOptions;

	return {
		fork<TBranches extends BranchRecord>(name: string, branches?: TBranches) {
			return createFork(step, name, resolvedOptions, branches);
		},
		join: {
			required: (fork, options) => joinRequired(fork, options),
			settled: (fork) => joinSettled(fork),
		},
	};
}

function createFork<TBranches extends BranchRecord | undefined>(
	rootStep: WorkflowStep,
	name: string,
	options: RequiredWorkflowOptions,
	branches: TBranches
): InternalFork<NonNullable<TBranches>>;
function createFork(
	rootStep: WorkflowStep,
	name: string,
	options: RequiredWorkflowOptions,
	branches?: BranchRecord
): InternalFork<BranchRecord> {
	const state: ForkState = {
		name,
		rootStep,
		options,
		branches: new Map(),
	};

	const fork: InternalFork<BranchRecord> = {
		name,
		branch(branchName, factory) {
			state.branches.set(branchName, factory);
			return fork as InternalFork<BranchRecord>;
		},
		[forkState]: state,
	};

	if (branches !== undefined) {
		for (const [branchName, factory] of Object.entries(branches)) {
			fork.branch(branchName, factory);
		}
	}

	return fork;
}

async function joinRequired<TBranches extends BranchRecord>(
	fork: Fork<TBranches>,
	options: RequiredJoinOptions = {}
): Promise<RequiredJoinResult<TBranches>> {
	const abortOnFailure = options.abortOnFailure ?? "none";
	await emitForkMarker(fork, "required", abortOnFailure);
	const outcomes = await runBranchesSettled(fork, abortOnFailure);
	await emitJoinMarker(fork, "required", abortOnFailure, outcomes);

	if (Object.values(outcomes).some((outcome) => outcome.status !== "success")) {
		throw new ForkJoinError({
			forkName: fork.name,
			outcomes: outcomes as Record<string, WorkflowOutcome<unknown>>,
		});
	}

	return successfulValues(outcomes) as RequiredJoinResult<TBranches>;
}

async function joinSettled<TBranches extends BranchRecord>(
	fork: Fork<TBranches>
): Promise<SettledJoinResult<TBranches>> {
	await emitForkMarker(fork, "settled", "none");
	const outcomes = await runBranchesSettled(fork, "none");
	await emitJoinMarker(fork, "settled", "none", outcomes);

	return outcomes;
}

async function runBranchesSettled<TBranches extends BranchRecord>(
	fork: Fork<TBranches>,
	abortOnFailure: AbortOnFailure
): Promise<SettledJoinResult<TBranches>> {
	const internalFork = fork as InternalFork<TBranches>;
	const state = internalFork[forkState];
	const run: ForkRun = {
		forkName: fork.name,
		abortOnFailure,
		controller: new AbortController(),
		abortReason: undefined,
	};

	const entries = await Promise.all(
		Array.from(state.branches.entries()).map(async ([branchName, factory]) => {
			const cancellation = createForkCancellation(run, branchName);
			const step = scopedStep(state, cancellation);

			try {
				const value = await factory({
					step,
					forkName: fork.name,
					branchName,
					signal: run.controller.signal,
					cancellation,
				});

				return [branchName, { status: "success", value }] as const;
			} catch (error) {
				if (error instanceof ForkAbortError) {
					return [
						branchName,
						{ status: "aborted", reason: error.reason },
					] as const;
				}

				if (run.abortOnFailure === "cooperative") {
					requestForkAbort(run, branchName);
				}

				return [branchName, { status: "failure", error }] as const;
			}
		})
	);

	return Object.fromEntries(entries) as SettledJoinResult<TBranches>;
}

function scopedStep(
	state: ForkState,
	cancellation: ForkCancellation
): ScopedWorkflowStep {
	const prefix = (name: string) =>
		`${state.name}${state.options.stepNameSeparator}${name}`;

	function doStep<T>(
		name: string,
		callback: (ctx: WorkflowStepContext) => Promise<T>,
		rollbackOptions?: WorkflowStepRollbackOptions<T>
	): Promise<Serializable<T>>;
	function doStep<T>(
		name: string,
		config: WorkflowStepConfig,
		callback: (ctx: WorkflowStepContext) => Promise<T>,
		rollbackOptions?: WorkflowStepRollbackOptions<T>
	): Promise<Serializable<T>>;
	function doStep<T>(
		name: string,
		configOrCallback:
			| WorkflowStepConfig
			| ((ctx: WorkflowStepContext) => Promise<T>),
		callbackOrRollback?:
			| ((ctx: WorkflowStepContext) => Promise<T>)
			| WorkflowStepRollbackOptions<T>,
		rollbackOptions?: WorkflowStepRollbackOptions<T>
	): Promise<Serializable<T>> {
		cancellation.throwIfRequested();

		return (typeof configOrCallback === "function"
			? state.rootStep.do(
					prefix(name),
					configOrCallback as (
						ctx: WorkflowStepContext
					) => Promise<Rpc.Serializable<T>>,
					callbackOrRollback as WorkflowStepRollbackOptions<Rpc.Serializable<T>>
				)
			: state.rootStep.do(
					prefix(name),
					configOrCallback,
					callbackOrRollback as (
						ctx: WorkflowStepContext
					) => Promise<Rpc.Serializable<T>>,
					rollbackOptions as WorkflowStepRollbackOptions<Rpc.Serializable<T>>
				)) as Promise<Serializable<T>>;
	}

	return {
		do: doStep,
		sleep: (name, duration) => {
			cancellation.throwIfRequested();
			return state.rootStep.sleep(prefix(name), duration);
		},
		sleepUntil: (name, timestamp) => {
			cancellation.throwIfRequested();
			return state.rootStep.sleepUntil(prefix(name), timestamp);
		},
		waitForEvent: (name, options) => {
			cancellation.throwIfRequested();
			return state.rootStep.waitForEvent(prefix(name), options);
		},
	};
}

function createForkCancellation(
	run: ForkRun,
	branchName: string
): ForkCancellation {
	return {
		signal: run.controller.signal,
		get requested() {
			return run.controller.signal.aborted;
		},
		get reason() {
			return run.abortReason;
		},
		throwIfRequested() {
			if (run.abortReason === undefined) {
				return;
			}

			throw new ForkAbortError({
				forkName: run.forkName,
				branchName,
				reason: run.abortReason,
			});
		},
	};
}

function requestForkAbort(run: ForkRun, sourceBranchName: string): void {
	if (run.abortReason !== undefined) {
		return;
	}

	run.abortReason = {
		type: "branch-failure",
		forkName: run.forkName,
		sourceBranchName,
	};
	run.controller.abort(run.abortReason);
}

async function emitForkMarker<TBranches extends BranchRecord>(
	fork: Fork<TBranches>,
	policy: JoinPolicy,
	abortOnFailure: AbortOnFailure
): Promise<void> {
	const state = (fork as InternalFork<TBranches>)[forkState];

	if (state.options.markers === "off") {
		return;
	}

	const markerName = `${state.name}${state.options.stepNameSeparator}fork`;

	if (state.options.markers === "minimal") {
		await state.rootStep.do(markerName, async () => undefined);
		return;
	}

	const marker: ForkMarker = {
		type: "fork",
		name: state.name,
		branches: Array.from(state.branches.keys()),
		policy,
		abortOnFailure,
	};

	await state.rootStep.do(markerName, async () => marker);
}

async function emitJoinMarker<TBranches extends BranchRecord>(
	fork: Fork<TBranches>,
	policy: JoinPolicy,
	abortOnFailure: AbortOnFailure,
	outcomes: Record<string, WorkflowOutcome<unknown>>
): Promise<void> {
	const state = (fork as InternalFork<TBranches>)[forkState];

	if (state.options.markers === "off") {
		return;
	}

	const markerName = `${state.name}${state.options.stepNameSeparator}join`;

	if (state.options.markers === "minimal") {
		await state.rootStep.do(markerName, async () => undefined);
		return;
	}

	const branches = Object.fromEntries(
		Object.entries(outcomes).map(([branchName, outcome]) => [
			branchName,
			outcome.status,
		])
	);

	const marker: JoinMarker = {
		type: "join",
		name: state.name,
		policy,
		abortOnFailure,
		status: Object.values(branches).some((status) => status !== "success")
			? "failure"
			: "success",
		branches,
	};

	await state.rootStep.do(markerName, async () => marker);
}

function successfulValues<TBranches extends BranchRecord>(
	outcomes: SettledJoinResult<TBranches>
): Partial<RequiredJoinResult<TBranches>> {
	const values: Partial<RequiredJoinResult<TBranches>> = {};

	for (const [branchName, outcome] of Object.entries(outcomes)) {
		if (outcome.status === "success") {
			values[branchName as keyof RequiredJoinResult<TBranches>] =
				outcome.value as RequiredJoinResult<TBranches>[keyof RequiredJoinResult<TBranches>];
		}
	}

	return values;
}
