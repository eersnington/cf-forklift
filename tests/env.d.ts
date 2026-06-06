import type { TestParams } from "./helpers/test-workflow";

interface TestEnv {
	CALL_LOG: KVNamespace;
	TEST_WORKFLOW: Workflow<TestParams>;
}

declare namespace Cloudflare {
	interface Env extends TestEnv {}
}

declare module "cloudflare:test" {
	interface ProvidedEnv extends TestEnv {}
}
