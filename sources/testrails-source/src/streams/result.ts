import {
  AirbyteLogger,
  AirbyteStreamBase,
  StreamKey,
  SyncMode,
} from 'faros-airbyte-cdk';
import {Dictionary} from 'ts-essentials';

import {Result} from '../models';
import {TestRails, TestRailsConfig} from '../testrails/testRails';

export interface ResultState {
  created_after: number;
}

export class Results extends AirbyteStreamBase {
  constructor(
    private readonly config: TestRailsConfig,
    protected readonly logger: AirbyteLogger
  ) {
    super(logger);
  }

  getJsonSchema(): Dictionary<any, string> {
    return require('../../resources/schemas/result.json');
  }
  get primaryKey(): StreamKey {
    return 'id';
  }
  get cursorField(): string | string[] {
    return 'created_on';
  }

  async *readRecords(
    syncMode: SyncMode,
    cursorField?: string[],
    streamSlice?: Dictionary<any, string>,
    streamState?: ResultState
  ): AsyncGenerator<Result> {
    const testRails = await TestRails.instance(this.config, this.logger);

    const isIncremental = syncMode === SyncMode.INCREMENTAL;
    yield* testRails.getResults(
      isIncremental ? streamState.created_after : undefined
    );
  }

  getUpdatedState(
    currentStreamState: ResultState,
    latestRecord: Result
  ): Dictionary<any> {
    return {
      created_after: Math.max(
        currentStreamState.created_after ?? 0,
        latestRecord.created_on ?? 0
      ),
    };
  }
}
