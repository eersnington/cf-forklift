import {
	WorkflowEntrypoint,
	type WorkflowEvent,
	type WorkflowStep,
} from "cloudflare:workers";
import { ForkJoinError, withWorkflow } from "../../src/index.ts";

export type TestScenario =
	| "required-success"
	| "required-failure-drains"
	| "minimal-markers";

export type TestParams = {
	scenario: TestScenario;
	testId: string;
};

type TestEnv = {
	CALL_LOG: KVNamespace;
};

export class TestWorkflow extends WorkflowEntrypoint<TestEnv, TestParams> {
	private callCounter = 0;

	override async run(
		event: WorkflowEvent<TestParams>,
		step: WorkflowStep
	): Promise<unknown> {
		this.callCounter = 0;

		switch (event.payload.scenario) {
			case "required-success":
				return this.requiredSuccess(event.payload.testId, step);
			case "required-failure-drains":
				return this.requiredFailureDrains(event.payload.testId, step);
			case "minimal-markers":
				return this.minimalMarkers(event.payload.testId, step);
		}
 
		event.payload.scenario satisfies never;
	}

	private async requiredSuccess(testId: string, step: WorkflowStep) {
		const workflow = withWorkflow(step);
		const fork = workflow.fork("verify merchant", {
			profile: ({ step }) =>
				step.do("verify profile", async () => {
					await this.log(testId, "verify merchant / verify profile");
					return "profile-ok";
				}),
			bank: ({ step }) =>
				step.do("verify bank", async () => {
					await this.log(testId, "verify merchant / verify bank");
					return "bank-ok";
				}),
		});

		return workflow.join.required(fork);
	}

	private async requiredFailureDrains(testId: string, step: WorkflowStep) {
		const workflow = withWorkflow(step);
		const fork = workflow.fork("verify merchant", {
			profile: ({ step }) =>
				step.do("verify profile", async () => {
					await this.log(testId, "verify merchant / verify profile");
					return "profile-ok";
				}),
			bank: ({ step }) =>
				step.do(
					"verify bank",
					{ retries: { limit: 0, delay: "1 second" } },
					async () => {
						await this.log(testId, "verify merchant / verify bank");
						throw new Error("bank failed");
					}
				),
			risk: ({ step }) =>
				step.do("screen risk", async () => {
					await this.log(testId, "verify merchant / screen risk");
					return "risk-ok";
				}),
		});

		try {
			await workflow.join.required(fork);
			throw new Error("required join should have failed");
		} catch (error) {
			if (error instanceof ForkJoinError) {
				await this.log(testId, "fork join error", {
					status: error.outcomes.bank?.status,
				});
				throw error;
			}

			throw error;
		}
	}

	private async minimalMarkers(testId: string, step: WorkflowStep) {
		const workflow = withWorkflow(step, { markers: "minimal" });
		const fork = workflow.fork("verify merchant", {
			profile: ({ step }) =>
				step.do("verify profile", async () => {
					await this.log(testId, "verify merchant / verify profile");
					return "profile-ok";
				}),
		});

		await workflow.join.required(fork);

		return { ok: true };
	}

	private async log(
		testId: string,
		stepName: string,
		data: Record<string, unknown> = {}
	): Promise<void> {
		const key = `${testId}:${String(this.callCounter++).padStart(5, "0")}:${stepName}`;
		await this.env.CALL_LOG.put(key, JSON.stringify(data));
	}
}

export default {
	async fetch(): Promise<Response> {
		return new Response("cf-forklift test worker");
	},
};
