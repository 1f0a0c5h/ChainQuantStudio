export type HandoffPhase = 'outbound'|'returning';

export function advanceHandoffPhase(phase:HandoffPhase):HandoffPhase|null {
  return phase==='outbound'?'returning':null;
}
