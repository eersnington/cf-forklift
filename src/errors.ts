import type { ForkAbortReason, WorkflowOutcome } from "./types";

/**
 * Error thrown by `workflow.join.required()` when one or more branches do not
 * complete successfully.
 *
 * Required joins drain every branch before throwing. Inspect `outcomes` to see
 * which branches succeeded, failed, or stopped after cooperative abort.
 *
 * @example
 * ```ts
 * try {
 * 	await workflow.join.required(checks);
 * } catch (error) {
 * 	if (error instanceof ForkJoinError) {
 * 		console.log(error.outcomes.bank?.status);
 * 	}
 * }
 * ```
 */
export class ForkJoinError extends Error {
	/** Name of the fork that did not complete successfully. */
	readonly forkName: string;

	/** Keyed outcomes for every drained branch. */
	readonly outcomes: Record<string, WorkflowOutcome<unknown>>;

	constructor(options: {
		readonly forkName: string;
		readonly outcomes: Record<string, WorkflowOutcome<unknown>>;
	}) {
		super(formatForkJoinErrorMessage(options));
		this.name = "ForkJoinError";
		this.forkName = options.forkName;
		this.outcomes = options.outcomes;
	}
}

/**
 * Error thrown by `cancellation.throwIfRequested()` after cooperative abort has
 * been requested.
 *
 * cf-forklift normally catches this error and records the branch outcome as
 * `aborted`, so application code usually uses it for `instanceof` checks rather
 * than constructing it directly.
 *
 * @example
 * ```ts
 * branch: async ({ step, cancellation }) => {
 * 	await step.do("first step", firstStep);
 * 	cancellation.throwIfRequested();
 * 	return await step.do("second step", secondStep);
 * }
 * ```
 */
export class ForkAbortError extends Error {
	/** Name of the fork whose branch stopped. */
	readonly forkName: string;

	/** Name of the branch that observed cooperative abort. */
	readonly branchName: string;

	/** Reason the branch stopped. */
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

function formatForkJoinErrorMessage(options: {
	readonly forkName: string;
	readonly outcomes: Record<string, WorkflowOutcome<unknown>>;
}): string {
	const failures = branchNamesByStatus(options.outcomes, "failure");
	const aborted = branchNamesByStatus(options.outcomes, "aborted");
	const details = [
		failures.length > 0 ? `Failed branches: ${failures.join(", ")}.` : undefined,
		aborted.length > 0 ? `Aborted branches: ${aborted.join(", ")}.` : undefined,
	]
		.filter((detail) => detail !== undefined)
		.join(" ");

	return `Fork "${options.forkName}" did not complete successfully. ${details} Completed branches were drained; inspect outcomes before retrying or compensating.`;
}

function branchNamesByStatus(
	outcomes: Record<string, WorkflowOutcome<unknown>>,
	status: WorkflowOutcome<unknown>["status"]
): string[] {
	return Object.entries(outcomes)
		.filter(([, outcome]) => outcome.status === status)
		.map(([name]) => name);
}
