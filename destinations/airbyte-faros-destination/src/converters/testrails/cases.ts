import {AirbyteRecord} from 'faros-airbyte-cdk';

import {DestinationModel, DestinationRecord, StreamContext} from '../converter';
import {TestRailsConverter} from './common';

export class Cases extends TestRailsConverter {
  readonly destinationModels: ReadonlyArray<DestinationModel> = [
    'qa_TestCase',
    'qa_TestSuiteTestCaseAssociation',
  ];

  async convert(
    record: AirbyteRecord,
    ctx: StreamContext
  ): Promise<ReadonlyArray<DestinationRecord>> {
    const res: DestinationRecord[] = [];
    const source = this.streamName.source;
    const testCase = record.record.data;

    const milestoneTag = `milestone:${testCase.milestone}`;

    res.push({
      model: 'qa_TestCase',
      record: {
        uid: testCase.id,
        name: testCase.title,
        source,
        tags: [milestoneTag],
        type: this.convertType(testCase.type),
      },
    });

    res.push({
      model: 'qa_TestSuiteTestCaseAssociation',
      record: {
        testSuite: {uid: testCase.uid, source},
        testCase: {uid: testCase.suite_id, source},
      },
    });

    return res;
  }
}
