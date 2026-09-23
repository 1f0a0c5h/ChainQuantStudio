export type HandoffPhase = 'outbound'|'returning';

export function advanceHandoffPhase(phase:HandoffPhase):HandoffPhase|null {
  return phase==='outbound'?'returning':null;
}

export function carriesHandoffPackage(phase:HandoffPhase):boolean {
  return phase==='outbound';
}
