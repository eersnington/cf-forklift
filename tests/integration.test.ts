import {
	env,
	introspectWorkflowInstance,
	type WorkflowInstanceIntrospector,
} from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { TestParams } from "./helpers/test-workflow";

type TestEnv = {
	CALL_LOG: KVNamespace;
	TEST_WORKFLOW: Workflow<TestParams>;
};

const testEnv = env as unknown as TestEnv;

type WorkflowRun = {
	testId: string;
	introspector: WorkflowInstanceIntrospector;
	dispose: () => Promise<void>;
};

async function waitForWorkflow(
	params: Omit<TestParams, "testId">,
	status: "complete" | "errored"
): Promise<WorkflowRun> {
	const testId = `test-${crypto.randomUUID()}`;
	const introspector = await introspectWorkflowInstance(
		testEnv.TEST_WORKFLOW,
		testId
	);

	await testEnv.TEST_WORKFLOW.create({
		id: testId,
		params: { ...params, testId },
	});

	await introspector.waitForStatus(status);

	return {
		testId,
		introspector,
		dispose: () => introspector.dispose(),
	};
}

async function getStepNames(testId: string): Promise<string[]> {
	const list = await testEnv.CALL_LOG.list({ prefix: `${testId}:` });

	return list.keys.map((key) => key.name.split(":").slice(2).join(":"));
}

describe("cf-forklift integration", () => {
	it("runs a required join with keyed branch results", async () => {
		const run = await waitForWorkflow({ scenario: "required-success" }, "complete");

		try {
			await expect(run.introspector.getOutput()).resolves.toEqual({
				profile: "profile-ok",
				bank: "bank-ok",
			});
			await expect(getStepNames(run.testId)).resolves.toEqual(expect.arrayContaining([
				"verify merchant / verify profile",
				"verify merchant / verify bank",
			]));
		} finally {
			await run.dispose();
		}
	});

	it("drains required join branches before failing", async () => {
		const run = await waitForWorkflow(
			{ scenario: "required-failure-drains" },
			"errored"
		);

		try {
			const error = await run.introspector.getError();
			expect(error.message).toContain("Fork \"verify merchant\" failed");
			await expect(
				run.introspector.waitForStepResult({ name: "verify merchant / join" })
			).resolves.toEqual({
				type: "join",
				name: "verify merchant",
				policy: "required",
				abortOnFailure: "none",
				status: "failure",
				branches: {
					profile: "success",
					bank: "failure",
					risk: "success",
				},
			});
			await expect(getStepNames(run.testId)).resolves.toEqual(expect.arrayContaining([
				"verify merchant / verify profile",
				"verify merchant / verify bank",
				"verify merchant / screen risk",
			]));
		} finally {
			await run.dispose();
		}
	});

	it("cooperatively aborts future branch work when requested", async () => {
		const run = await waitForWorkflow(
			{ scenario: "required-cooperative-abort" },
			"errored"
		);

		try {
			const error = await run.introspector.getError();
			expect(error.message).toContain("Fork \"verify merchant\" failed");
			await expect(
				run.introspector.waitForStepResult({ name: "verify merchant / fork" })
			).resolves.toEqual({
				type: "fork",
				name: "verify merchant",
				branches: ["bank", "risk"],
				policy: "required",
				abortOnFailure: "cooperative",
			});
			await expect(
				run.introspector.waitForStepResult({ name: "verify merchant / join" })
			).resolves.toEqual({
				type: "join",
				name: "verify merchant",
				policy: "required",
				abortOnFailure: "cooperative",
				status: "failure",
				branches: {
					bank: "failure",
					risk: "aborted",
				},
			});
			const stepNames = await getStepNames(run.testId);
			expect(stepNames).toEqual(
				expect.arrayContaining(["verify merchant / verify bank"])
			);
			expect(stepNames).not.toContain("verify merchant / risk followup");
		} finally {
			await run.dispose();
		}
	});

	it("emits minimal marker steps", async () => {
		const run = await waitForWorkflow({ scenario: "minimal-markers" }, "complete");

		try {
			await expect(
				run.introspector.waitForStepResult({ name: "verify merchant / fork" })
			).resolves.toBeUndefined();
			await expect(
				run.introspector.waitForStepResult({ name: "verify merchant / join" })
			).resolves.toBeUndefined();
			await expect(getStepNames(run.testId)).resolves.toEqual([
				"verify merchant / verify profile",
			]);
		} finally {
			await run.dispose();
		}
	});
});
