import assert from 'node:assert/strict';
import test from 'node:test';

import {advanceHandoffPhase, carriesHandoffPackage} from '../app/handoff-animation.ts';

test('a handoff returns the agent to their desk before completing',()=>{
  const returnPhase=advanceHandoffPhase('outbound');
  assert.equal(returnPhase,'returning');
  assert.equal(carriesHandoffPackage('outbound'),true);
  assert.equal(carriesHandoffPackage(returnPhase),false);
  assert.equal(advanceHandoffPhase(returnPhase),null);
});
