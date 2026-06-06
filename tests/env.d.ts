import type { TestParams } from "./helpers/test-workflow";

declare global {
	namespace Cloudflare {
		interface Env {
			CALL_LOG: KVNamespace;
			TEST_WORKFLOW: Workflow<TestParams>;
		}
	}
}

export {};
