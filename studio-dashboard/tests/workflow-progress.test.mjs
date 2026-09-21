import assert from 'node:assert/strict';
import test from 'node:test';

import {getWorkflowProgress} from '../app/workflow-progress.ts';

test('wall progress reports the current workflow stage',()=>{
  assert.deepEqual(getWorkflowProgress(null),{label:'NO ACTIVE WORK',percent:0,tone:'idle'});
  assert.equal(getWorkflowProgress({stage:'review_backtest',status:'working'}).label,'BACKTEST REVIEW');
  assert.equal(getWorkflowProgress({stage:'optimization_approval',status:'awaiting_optimization_approval'}).percent,82);
  assert.deepEqual(getWorkflowProgress({stage:'ready_for_signal',status:'completed'}),{label:'COMPLETE',percent:100,tone:'done'});
  assert.equal(getWorkflowProgress({stage:'backtest',status:'failed'}).tone,'alert');
});
