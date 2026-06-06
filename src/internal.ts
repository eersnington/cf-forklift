import type {
	WorkflowStep,
	WorkflowStepConfig,
	WorkflowStepContext,
	WorkflowStepRollbackOptions,
} from "cloudflare:workers";
import { ForkAbortError, ForkJoinError } from "./errors";
import {
	forkState,
	type AbortOnFailure,
	type BranchRecord,
	type Fork,
	type ForkCancellation,
	type ForkMarker,
	type ForkRun,
	type ForkState,
	type InternalFork,
	type JoinMarker,
	type JoinPolicy,
	type RequiredJoinOptions,
	type RequiredJoinResult,
	type RequiredWorkflowOptions,
	type ScopedWorkflowStep,
	type Serializable,
	type SettledJoinResult,
	type Workflow,
	type WorkflowOptions,
	type WorkflowOutcome,
} from "./types";

/**
 * Creates a fork/join helper around a Cloudflare WorkflowStep.
 *
 * Use this once inside a Workflow `run()` method, then create forks and join
 * them with either `join.required()` or `join.settled()`.
 *
 * @example
 * ```ts
 * export class MerchantWorkflow extends WorkflowEntrypoint<Env, { id: string }> {
 * 	async run(event: WorkflowEvent<{ id: string }>, step: WorkflowStep) {
 * 		const workflow = withWorkflow(step);
 *
 * 		const checks = workflow.fork("verify merchant", {
 * 			profile: ({ step }) => step.do("verify profile", () => verifyProfile(event.payload.id)),
 * 			bank: ({ step }) => step.do("verify bank", () => verifyBank(event.payload.id)),
 * 		});
 *
 * 		return await workflow.join.required(checks);
 * 	}
 * }
 * ```
 */
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
			if (state.branches.has(branchName)) {
				throw new Error(
					`Fork "${state.name}" already has a branch named "${branchName}". Branch names become output keys and must be unique within a fork.`
				);
			}

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
			const step = scopedStep(state, branchName, cancellation);

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
	branchName: string,
	cancellation: ForkCancellation
): ScopedWorkflowStep {
	const prefix = (name: string) =>
		`${state.name}${state.options.stepNameSeparator}${branchName}${state.options.stepNameSeparator}${name}`;

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
