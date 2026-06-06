
# cf-forklift

Structured fork/join parallelism helpers for Cloudflare Workflows.

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
		const workflow = withWorkflow(step, {
			markers: "minimal",
		});

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
	markers?: "off" | "minimal";
	defaultRequiredFailureMode?: "throwAfterDrain" | "failFast";
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
await workflow.join.settled(fork);
```

## Native Rollbacks

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

## How It Works

cf-forklift wraps WorkflowStep and prefixes branch step names.

```
verify merchant / verify profile
verify merchant / verify bank
verify merchant / screen risk
```

## Optional Markers

Add real Workflow steps around a fork/join:

```
verify merchant / fork
verify merchant / verify profile
verify merchant / verify bank
verify merchant / join
```

Cloudflare still records primitive Workflow steps. cf-forklift adds structured naming, keyed outputs, and join policies in userland.

## Important Notes

- Forks do not start branches until joined.
- required waits for all branches before throwing by default.
- settled returns keyed success/failure outcomes.
- waitForEvent remains Cloudflare's native step.waitForEvent; timeout-as-value helpers are not part of v1.
- Branch names and step names should be deterministic.
- firstCompleted / race semantics are intentionally not part of the initial API.

## License

MIT
