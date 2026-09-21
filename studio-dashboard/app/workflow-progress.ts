export type WorkflowProgressSource = {stage?:string;status:string};
export type WorkflowProgress = {label:string;percent:number;tone:'idle'|'active'|'done'|'alert'};

const stages:Array<[string[],string,number]> = [
  [['manager','created','queued'],'MANAGER',8],
  [['strategy'],'STRATEGY',18],
  [['review_implementation'],'IMPLEMENTATION REVIEW',30],
  [['implementation_approval','awaiting_implementation_approval'],'IMPLEMENTATION APPROVAL',40],
  [['data'],'DATA SERVICE',48],
  [['backtest'],'BACKTEST',58],
  [['review_backtest'],'BACKTEST REVIEW',68],
  [['optimize'],'OPTIMIZATION',76],
  [['optimization_approval','awaiting_optimization_approval'],'OPTIMIZATION DECISION',82],
  [['backtest_approval','awaiting_backtest_approval'],'BACKTEST APPROVAL',88],
  [['live_approval','awaiting_live_approval'],'LIVE APPROVAL',94],
  [['ready_for_signal','signal'],'SIGNAL OPS',100],
];

export function getWorkflowProgress(order:WorkflowProgressSource|null):WorkflowProgress {
  if(!order)return {label:'NO ACTIVE WORK',percent:0,tone:'idle'};
  if(['failed','dead_letter','blocked'].includes(order.status))return {label:'NEEDS ATTENTION',percent:0,tone:'alert'};
  if(order.status==='cancelled')return {label:'CANCELLED',percent:0,tone:'alert'};
  if(order.status==='completed')return {label:'COMPLETE',percent:100,tone:'done'};
  const marker=`${order.stage??''} ${order.status}`;
  for(const [keys,label,percent] of stages.slice().reverse()){
    if(keys.some((key)=>marker.includes(key)))return {label,percent,tone:percent===100?'done':'active'};
  }
  return {label:'WORK QUEUED',percent:5,tone:'active'};
}
