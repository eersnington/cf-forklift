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

type RequiredFailureMode = "throwAfterDrain" | "failFast";

type MarkerMode = "off" | "minimal";

export type WorkflowOptions = {
	readonly stepNameSeparator?: string;
	readonly markers?: MarkerMode;
	readonly defaultRequiredFailureMode?: RequiredFailureMode;
};

export type WorkflowOutcome<T> =
	| { readonly status: "success"; readonly value: T }
	| { readonly status: "failure"; readonly error: unknown };

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

type ForkState = {
	readonly name: string;
	readonly rootStep: WorkflowStep;
	readonly options: RequiredWorkflowOptions;
	readonly branches: Map<string, BranchFactory<unknown>>;
};

type RequiredWorkflowOptions = {
	readonly stepNameSeparator: string;
	readonly markers: MarkerMode;
	readonly defaultRequiredFailureMode: RequiredFailureMode;
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
			options?: { readonly failureMode?: RequiredFailureMode }
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

export function withWorkflow(
	step: WorkflowStep,
	options: WorkflowOptions = {}
): Workflow {
	const resolvedOptions = {
		stepNameSeparator: options.stepNameSeparator ?? " / ",
		markers: options.markers ?? "off",
		defaultRequiredFailureMode:
			options.defaultRequiredFailureMode ?? "throwAfterDrain",
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
	options: { readonly failureMode?: RequiredFailureMode } = {}
): Promise<RequiredJoinResult<TBranches>> {
	const internalFork = fork as InternalFork<TBranches>;
	const failureMode =
		options.failureMode ?? internalFork[forkState].options.defaultRequiredFailureMode;

	if (failureMode === "failFast") {
		await emitMarker(fork, "fork");
		const values = await runBranches(fork);
		await emitMarker(fork, "join");
		return values as RequiredJoinResult<TBranches>;
	}

	const outcomes = await joinSettled(fork);

	if (Object.values(outcomes).some((outcome) => outcome.status === "failure")) {
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
	const internalFork = fork as InternalFork<TBranches>;
	const state = internalFork[forkState];
	const step = scopedStep(state);

	await emitMarker(fork, "fork");

	const entries = await Promise.all(
		Array.from(state.branches.entries()).map(async ([branchName, factory]) => {
			try {
				const value = await factory({
					step,
					forkName: fork.name,
					branchName,
				});

				return [branchName, { status: "success", value }] as const;
			} catch (error) {
				return [branchName, { status: "failure", error }] as const;
			}
		})
	);

	await emitMarker(fork, "join");

	return Object.fromEntries(entries) as SettledJoinResult<TBranches>;
}

async function runBranches<TBranches extends BranchRecord>(
	fork: Fork<TBranches>
): Promise<Record<string, unknown>> {
	const internalFork = fork as InternalFork<TBranches>;
	const state = internalFork[forkState];
	const step = scopedStep(state);

	const entries = await Promise.all(
		Array.from(state.branches.entries()).map(
			async ([branchName, factory]) => [
				branchName,
				await factory({
					step,
					forkName: fork.name,
					branchName,
				}),
			]
		)
	);

	return Object.fromEntries(entries);
}

function scopedStep(state: ForkState): ScopedWorkflowStep {
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
		sleep: (name, duration) => state.rootStep.sleep(prefix(name), duration),
		sleepUntil: (name, timestamp) =>
			state.rootStep.sleepUntil(prefix(name), timestamp),
		waitForEvent: (name, options) =>
			state.rootStep.waitForEvent(prefix(name), options),
	};
}

async function emitMarker<TBranches extends BranchRecord>(
	fork: Fork<TBranches>,
	marker: "fork" | "join"
): Promise<void> {
	const state = (fork as InternalFork<TBranches>)[forkState];

	if (state.options.markers === "off") {
		return;
	}

	await state.rootStep.do(
		`${state.name}${state.options.stepNameSeparator}${marker}`,
		async () => undefined
	);
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
