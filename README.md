
# cf-forklift

Netflix Conductor-style structured fork/join parallelism helpers for [Cloudflare Workflows](https://developers.cloudflare.com/workflows/).

## Installation

```bash
npm install cf-forklift
pnpm add cf-forklift
bun add cf-forklift
```

## Usage

```ts
import { withWorkflow } from "cf-forklift";
import {
	WorkflowEntrypoint,
	type WorkflowEvent,
	type WorkflowStep,
} from "cloudflare:workers";

export class MerchantWorkflow extends WorkflowEntrypoint<
	Env,
	{ merchantId: string }
> {
	async run(event: WorkflowEvent<{ merchantId: string }>, step: WorkflowStep) {
		const workflow = withWorkflow(step);

		const merchantId = event.payload.merchantId;

		const verifyMerchant = workflow.fork("verify merchant", {
			profile: ({ step }) =>
				step.do("verify profile", () => verifyProfile(merchantId)),

			bank: ({ step }) =>
				step.do("verify bank", () => verifyBank(merchantId)),

			risk: ({ step }) =>
				step.do("screen risk", () => screenRisk(merchantId)),
		});

		const verification = await workflow.join.required(verifyMerchant);

		const enrichMerchant = workflow.fork("enrich merchant", {
			website: ({ step }) =>
				step.do("check website reputation", () => checkWebsite(merchantId)),

			marketplace: ({ step }) =>
				step.do("check marketplace footprint", () =>
					checkMarketplace(merchantId)
				),

			support: ({ step }) =>
				step.do("check support profile", () =>
					checkSupportProfile(merchantId)
				),
		});

		const enrichment = await workflow.join.settled(enrichMerchant);

		return {
			merchantId,
			verification,
			enrichment,
		};
	}
}
```

## API

```ts
const workflow = withWorkflow(step, options?);
```

withWorkflow Options
```ts
type Options = {
	stepNameSeparator?: string;
	markers?: "off" | "minimal" | "summary";
};
```

Fork
```ts
const fork = workflow.fork("verify merchant", {
	profile: ({ step }) => step.do("verify profile", verifyProfile),
	bank: ({ step }) => step.do("verify bank", verifyBank),
});
```

Dynamic forks:
```ts
const fork = workflow.fork("run checks");

for (const check of checks) {
	fork.branch(check.name, ({ step }) =>
		step.do(check.stepName, () => runCheck(check))
	);
}
```

Join
```ts
await workflow.join.required(fork);
await workflow.join.required(fork, { abortOnFailure: "cooperative" });
await workflow.join.settled(fork);
```

`required` returns keyed values when every branch succeeds. `settled` returns keyed branch outcomes without throwing for branch failures.

## Supports Native Rollbacks

```ts
const provision = workflow.fork("provision resources", {
	resource: ({ step }) =>
		step.do(
			"provision resource",
			async () => {
				const resource = await provisionResource();
				return { resourceId: resource.id };
			},
			{
				rollback: async ({ output }) => {
					const { resourceId } = output as { resourceId: string };
					await deleteResource(resourceId);
				},
				rollbackConfig: {
					retries: { limit: 3, delay: "15 seconds", backoff: "linear" },
					timeout: "2 minutes",
				},
			}
		),
});

await workflow.join.required(provision);
```

Register native Cloudflare rollback handlers on branch `step.do` calls that complete side effects. Rollbacks compensate completed work if the Workflow later fails. Cooperative abort does not undo completed work.

## Cooperative Abort

Required joins drain by default. If you want sibling branches to skip future work after one branch fails, opt into cooperative abort:

```ts
const verifyMerchant = workflow.fork("verify merchant", {
	bank: ({ step }) =>
		step.do("verify bank", async () => {
			throw new Error("bank failed");
		}),

	risk: async ({ step, cancellation }) => {
		cancellation.throwIfRequested();

		await step.do("risk check 1", async () => "ok");

		cancellation.throwIfRequested();

		return step.do(
			"risk check 2",
			async () => createRiskDecision(),
			{
				// Register rollback for side effects that may have completed before the fork fails.
				rollback: async ({ output }) => deleteRiskDecision(output),
			}
		);
	},
});

await workflow.join.required(verifyMerchant, {
	abortOnFailure: "cooperative",
});
```

Cooperative abort is a branch checkpoint mechanism. It is useful when later branch work should be skipped after another branch has already failed.

It does not change Cloudflare runtime behavior:

- Already-started `step.do`, `sleep`, `sleepUntil`, and `waitForEvent` calls still finish normally.
- Branches only stop when they reach `cancellation.throwIfRequested()` or start a scoped Workflow primitive after abort was requested.
- The required join still waits for every branch to settle before throwing `ForkJoinError`.
- Use native rollback handlers for side effects that may complete before the fork fails.

## How It Works

cf-forklift wraps WorkflowStep and prefixes branch step names.

```
verify merchant / profile / verify profile
verify merchant / bank / verify bank
verify merchant / risk / screen risk
```

Branch names are part of the Cloudflare step name so different branches can safely use the same local step name.

## Markers

Summary markers are enabled by default. They add real Workflow steps around a fork/join:

```
verify merchant / fork
verify merchant / profile / verify profile
verify merchant / bank / verify bank
verify merchant / join
```

The fork marker returns:

```ts
{
	type: "fork",
	name: "verify merchant",
	branches: ["profile", "bank"],
	policy: "required",
	abortOnFailure: "none",
}
```

The join marker returns:

```ts
{
	type: "join",
	name: "verify merchant",
	policy: "required",
	abortOnFailure: "none",
	status: "success",
	branches: {
		profile: "success",
		bank: "success",
	},
}
```

Use `markers: "minimal"` for breadcrumb-only marker steps, or `markers: "off"` to disable marker steps.

Cloudflare still records primitive Workflow steps. cf-forklift adds structured naming, keyed outputs, and join policies in userland.

## Behavior Guarantees

- Forks are lazy; branches do not start until a join method is called.
- `join.required(fork)` starts every branch, waits for every branch to settle, and throws `ForkJoinError` if any branch does not succeed.
- `join.required(fork, { abortOnFailure: "cooperative" })` requests cooperative abort after the first branch failure and still drains all branches.
- `join.settled(fork)` returns keyed `success`, `failure`, or `aborted` outcomes.
- Marker steps are enabled by default with `markers: "summary"`; use `"minimal"` or `"off"` to reduce Workflow history entries.
- Branch names must be unique within a fork because they become output keys.
- Branch names and step names should be deterministic because they become Cloudflare step names.

## License

MIT
