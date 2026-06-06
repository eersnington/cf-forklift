import type { Workflow, WorkflowOutcome } from "../src/index.ts";

type Assert<T extends true> = T;
type Equal<TActual, TExpected> = (<T>() => T extends TActual ? 1 : 2) extends <
	T,
>() => T extends TExpected ? 1 : 2
	? true
	: false;

type CheckResult = {
	ok: boolean;
};

declare const workflow: Workflow;
declare const check: {
	name: string;
	stepName: string;
};

const staticFork = workflow.fork("verify merchant", {
	bank: async () => ({ bank: true as const }),
	risk: async () => ({ risk: true as const }),
});

const staticRequired = workflow.join.required(staticFork);
type StaticRequired = Awaited<typeof staticRequired>;

const chainedFork = workflow
	.fork("verify merchant")
	.branch("bank", async () => ({ bank: true as const }))
	.branch("risk", async () => ({ risk: true as const }));

const chainedRequired = workflow.join.required(chainedFork);
type ChainedRequired = Awaited<typeof chainedRequired>;

const dynamicFork = workflow.fork<CheckResult>("run checks");

dynamicFork.branch(check.name, async () => ({ ok: true }));
dynamicFork.branch(check.name, ({ step }) =>
	step.do(check.stepName, async () => ({ ok: false }))
);
// @ts-expect-error Dynamic homogeneous forks require every branch to return the declared result type.
dynamicFork.branch(check.name, async () => ({ ok: "no" }));

const dynamicSettled = workflow.join.settled(dynamicFork);
type DynamicSettled = Awaited<typeof dynamicSettled>;

const dynamicRequired = workflow.join.required(dynamicFork);
type DynamicRequired = Awaited<typeof dynamicRequired>;

export type TypeRegressionAssertions = [
	Assert<
		Equal<
			StaticRequired,
			{
				bank: { bank: true };
				risk: { risk: true };
			}
		>
	>,
	Assert<Equal<ChainedRequired["bank"], { bank: true }>>,
	Assert<Equal<ChainedRequired["risk"], { risk: true }>>,
	Assert<
		Equal<DynamicSettled, Partial<Record<string, WorkflowOutcome<CheckResult>>>>
	>,
	Assert<Equal<DynamicRequired, Partial<Record<string, CheckResult>>>>,
];
