import {
	WorkflowEntrypoint,
	type WorkflowEvent,
	type WorkflowStep,
} from "cloudflare:workers";
import { ForkJoinError, withWorkflow } from "../../src/index.ts";

export type TestScenario =
	| "required-success"
	| "required-failure-drains"
	| "required-cooperative-abort"
	| "duplicate-branch-name"
	| "minimal-markers";

export type TestParams = {
	scenario: TestScenario;
	testId: string;
};

type TestEnv = {
	CALL_LOG: KVNamespace;
};

type FailureResult = {
	status: "failed";
	message: string;
	outcomes: Record<string, "success" | "failure" | "aborted">;
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
			case "required-cooperative-abort":
				return this.requiredCooperativeAbort(event.payload.testId, step);
			case "duplicate-branch-name":
				return this.duplicateBranchName(step);
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
					await this.log(testId, "verify merchant / profile / verify profile");
					return "profile-ok";
				}),
			bank: ({ step }) =>
				step.do("verify bank", async () => {
					await this.log(testId, "verify merchant / bank / verify bank");
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
					await this.log(testId, "verify merchant / profile / verify profile");
					return "profile-ok";
				}),
			bank: async ({ step }) => {
				await step.do("verify bank", async () => {
					await this.log(testId, "verify merchant / bank / verify bank");
					return "bank-ok";
				});

				throw new Error("bank failed");
			},
			risk: ({ step }) =>
				step.do("screen risk", async () => {
					await this.log(testId, "verify merchant / risk / screen risk");
					return "risk-ok";
				}),
		});

		try {
			return await workflow.join.required(fork);
		} catch (error) {
			if (error instanceof ForkJoinError) {
				return this.forkFailure(error);
			}

			throw error;
		}
	}

	private async requiredCooperativeAbort(testId: string, step: WorkflowStep) {
		const workflow = withWorkflow(step);
		const fork = workflow.fork("verify merchant", {
			bank: async ({ step }) => {
				await step.do("verify bank", async () => {
					await this.log(testId, "verify merchant / bank / verify bank");
					return "bank-ok";
				});

				throw new Error("bank failed");
			},
			risk: async ({ step, cancellation }) => {
				await step.sleep("wait before risk followup", "1 second");

				cancellation.throwIfRequested();

				return step.do("risk followup", async () => {
					await this.log(testId, "verify merchant / risk / risk followup");
					return "risk-ok";
				});
			},
		});

		try {
			return await workflow.join.required(fork, {
				abortOnFailure: "cooperative",
			});
		} catch (error) {
			if (error instanceof ForkJoinError) {
				return this.forkFailure(error);
			}

			throw error;
		}
	}

	private duplicateBranchName(step: WorkflowStep) {
		const workflow = withWorkflow(step);
		const fork = workflow.fork("verify merchant");

		try {
			fork.branch("bank", ({ step }) =>
				step.do("verify bank", async () => "bank-ok")
			);
			fork.branch("bank", ({ step }) =>
				step.do("verify bank again", async () => "bank-ok")
			);

			return { status: "unexpected-success" };
		} catch (error) {
			return {
				status: "failed",
				message: error instanceof Error ? error.message : String(error),
			};
		}
	}

	private async minimalMarkers(testId: string, step: WorkflowStep) {
		const workflow = withWorkflow(step, { markers: "minimal" });
		const fork = workflow.fork("verify merchant", {
			profile: ({ step }) =>
				step.do("verify profile", async () => {
					await this.log(testId, "verify merchant / profile / verify profile");
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

	private forkFailure(error: ForkJoinError): FailureResult {
		return {
			status: "failed",
			message: error.message,
			outcomes: Object.fromEntries(
				Object.entries(error.outcomes).map(([name, outcome]) => [
					name,
					outcome.status,
				])
			),
		};
	}
}

export default {
	async fetch(): Promise<Response> {
		return new Response("cf-forklift test worker");
	},
};
