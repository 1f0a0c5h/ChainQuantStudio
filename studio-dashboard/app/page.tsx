'use client';

import type { CSSProperties, FormEvent } from 'react';
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import Image from 'next/image';
import {advanceHandoffPhase, carriesHandoffPackage, type HandoffPhase} from './handoff-animation';
import {getWorkflowProgress} from './workflow-progress';

type AgentId = 'manager' | 'strategy' | 'backtest' | 'review' | 'optimize' | 'signal' | 'maintenance' | 'trading';
type AgentStatus = 'ONLINE' | 'STANDBY' | 'STOPPED' | 'LOCKED' | 'WORKING' | 'RUNNING' | 'QUEUED' | 'REVIEW' | 'BLOCKED';
type StudioAgent = { id:AgentId; number:string; name:string; role:string; accent:string; desk:string; current:string; handoff:string; sprite:string };
type AgentRuntime = { status:AgentStatus; task:string; progress:number };
type Handoff = { from:AgentId; to:AgentId; label:string; key:number };
type HandoffMotion = { startX:number; startY:number; endX:number; endY:number; duration:number };
type ChatAuthor = 'YOU'|'MANAGER'|'STRATEGY'|'BACKTEST'|'REVIEW'|'OPTIMIZE'|'MAINTENANCE';
type ChatMessage = { id:number|string; at?:string; author:ChatAuthor; text:string };
type ReportArtifact = {label:string;kind:'report'|'data'|'chart';name:string;url:string};
type BacktestFile = {title:string;status:string;candle_count:number|null;artifacts:ReportArtifact[]};
type ApprovalKey = 'implementation'|'optimization'|'backtest'|'live';
type ApprovalDecision = {approved:boolean;note:string;at:string};
type DagNode = {node_id:string;kind:string;requires:string[];owner:string};
type WorkOrder = {id:string;agent:AgentId;artifact_kind?:'signal'|'strategy'|'maintenance';artifact_id?:string;operation?:'create'|'modify';backtest_required?:boolean;optimization_required?:boolean;status:string;stage?:string;created_at:string;results?:Record<string,string>;backtest_file?:BacktestFile;approvals?:Record<ApprovalKey,ApprovalDecision|null>;dag?:{nodes:DagNode[]};checkpoints?:Array<{stage:string;status:string;at:string}>};
type ArtifactRecord = {work_order:string;artifact_kind:string;spec_sha256:string;code_revision:string;dataset_sha256:string|null;tests:string[];reports:string[];recorded_at:string};
type DatasetVersion = {key:string;content_sha256:string;source:string;created_at:string;rows?:number};
type LiveRegistryEntry = {id:string;kind:'signal'|'strategy';version:string|null;running:boolean;status:string};
type UserBlocker = {code:string;detail:string;reported_at:string};
type CodexHealth = {ok:boolean;ready:boolean;reason:string;login_supported:boolean;login_in_progress:boolean;at:string};
type ApiSnapshot = {
  connected:boolean; generated_at:string;
  agents:Record<AgentId,AgentRuntime&{executable:boolean;execution:string}>;
  signal_runtime:{running:boolean;pid:number|null;detail:string;log_path:string};
  messages:Array<{id:number;at:string;author:ChatAuthor;text:string}>;
  work_orders:WorkOrder[];
  dead_letters?:WorkOrder[];
  codex_circuit?:{state:string;open_until:string|null;reason:string|null};
  user_blockers?:UserBlocker[];
  live_registry?:LiveRegistryEntry[];
  artifact_registry?:ArtifactRecord[];
  data_service?:{datasets:DatasetVersion[]};
  trading_enabled:false;
};
type ManagerResponse = { reply:string;denied:boolean;handoff:Exclude<AgentId,'manager'>|null;action:string|null;decision:{action:string;target_agent:string|null;confidence:number;rationale:string};snapshot:ApiSnapshot;uploaded_spec?:{name:string;path:string;bytes:number;backtest_required:boolean};uploaded_attachments?:Array<{name:string;path:string;bytes:number;content_type:string}> };
type ConnectionState = 'CONNECTING'|'CONNECTED'|'OFFLINE';

const gatewayBase = 'http://127.0.0.1:8765';
const agents:StudioAgent[] = [
  {id:'manager',number:'01',name:'Manager Agent',role:'STUDIO MANAGER',accent:'#6ed8ff',desk:'Manager Desk',current:'Semantic coordinator with a strict four-capability allowlist.',handoff:'Artifact kind, task owner, progress, decisions, blockers, evidence, and next action',sprite:'cyan'},
  {id:'strategy',number:'02',name:'Strategy Agent',role:'RESEARCH DEV',accent:'#52d6c8',desk:'Research Desk',current:'Builds neutral Signal Definitions and directional Trading Strategies as separate artifacts.',handoff:'Versioned Signal or Strategy, horizon, normalized spec, tests, and data contract',sprite:'teal'},
  {id:'backtest',number:'03',name:'Backtest Agent',role:'BACKTEST LAB',accent:'#b998ff',desk:'Simulation Lab',current:'The reproducible qsa-backtest runner is ready.',handoff:'Reproducible manifest, performance charts, limitations, and approval package',sprite:'violet'},
  {id:'review',number:'04',name:'Review Agent',role:'INDEPENDENT REVIEW',accent:'#ffdc73',desk:'Review Office',current:'Independently checks specs, tests, look-ahead risk, evidence, and release readiness.',handoff:'Pass/fail findings, evidence gaps, look-ahead audit, and gate recommendation',sprite:'yellow'},
  {id:'optimize',number:'05',name:'Optimization Agent',role:'ROBUSTNESS RESEARCH',accent:'#ff8fcf',desk:'Optimization Lab',current:'Diagnoses reviewed backtests and proposes bounded, user-approved revisions.',handoff:'Evidence-backed diagnosis, bounded proposal, overfit guards, risks, and retest criteria',sprite:'pink'},
  {id:'signal',number:'06',name:'Signal Agent',role:'SIGNAL OPS',accent:'#73e58f',desk:'Signal Console',current:'Runs approved direction-neutral Signal Definitions on Binance public data.',handoff:'Structured neutral SIGNAL events, health metrics, and lifecycle reports',sprite:'green'},
  {id:'maintenance',number:'07',name:'Maintenance Agent',role:'RELIABILITY',accent:'#ffb45e',desk:'Repair Bay',current:'Diagnostics and Codex repair work orders are available.',handoff:'Root cause, minimal patch, replay evidence, monitoring results, and risk',sprite:'orange'},
  {id:'trading',number:'08',name:'Trading Agent',role:'TRADING DESK',accent:'#a8b0b9',desk:'Locked Desk',current:'No credentials, authenticated endpoint, signal consumer, or order capability.',handoff:'Unavailable; this signal-only repository cannot execute trades',sprite:'slate'},
];
const initialRuntime:Record<AgentId,AgentRuntime> = {
  manager:{status:'STOPPED',task:'Connecting to local Studio Gateway',progress:0},strategy:{status:'STANDBY',task:'Waiting for gateway',progress:0},backtest:{status:'STANDBY',task:'Waiting for gateway',progress:0},review:{status:'STANDBY',task:'Waiting for evidence',progress:0},optimize:{status:'STANDBY',task:'Waiting for reviewed backtest',progress:0},signal:{status:'STOPPED',task:'Runtime status unavailable',progress:0},maintenance:{status:'STANDBY',task:'Waiting for gateway',progress:0},trading:{status:'LOCKED',task:'Execution disabled',progress:0},
};
const pipeline = [['01','MANAGER','Compose an optional work DAG'],['02','STRATEGY','Implement and test the artifact'],['03','REVIEW','Independent implementation review'],['G1','IMPLEMENT','Explicit implementation approval'],['04','DATA + BACKTEST','Versioned data; optional replay'],['05','OPTIMIZE','Diagnose; propose only'],['G2','OPTIMIZE','User approves revision scope'],['G3','BACKTEST','Explicit evidence approval'],['G4','LIVE','Explicit release approval'],['06','SIGNAL OPS','Run approved definitions']];

function PixelWorker({agent,moving=false,carryingPackage=false}:{agent:StudioAgent;moving?:boolean;carryingPackage?:boolean}) {
  return <div className={`pixel-worker sprite-${agent.sprite} ${moving?'is-walking':''}`} aria-hidden="true"><span className="pixel-shadow"/>{!moving&&<span className="pixel-chair"/>}<span className="pixel-leg pixel-leg-left"/><span className="pixel-leg pixel-leg-right"/><span className="pixel-body"/><span className="pixel-neck"/><span className="pixel-head"/><span className="pixel-hair"/><span className="pixel-face"/><span className="pixel-arm arm-left"/><span className="pixel-arm arm-right"/>{moving&&carryingPackage&&<span className="pixel-briefcase">▣</span>}</div>;
}
function Workstation({agent,runtime,selected,departing,onSelect}:{agent:StudioAgent;runtime:AgentRuntime;selected:boolean;departing:boolean;onSelect:()=>void}) {
  return <button data-agent-id={agent.id} className={`workstation station-${agent.id} ${selected?'is-selected':''} ${departing?'is-handoff-source':''} ${runtime.status==='LOCKED'?'is-locked':''}`} style={{'--agent-accent':agent.accent} as CSSProperties} onClick={onSelect} aria-label={`View ${agent.name}, status ${runtime.status}`} aria-pressed={selected}>
    {(runtime.status==='WORKING'||runtime.status==='RUNNING')&&<span className="work-bubble" aria-live="polite"><b>{runtime.task}</b><small>{runtime.progress}%</small><i><em style={{width:`${runtime.progress}%`}}/></i></span>}
    <span className="agent-nameplate"><b>{agent.name}</b><em>{runtime.status}</em></span><span className="desk-shell"><span className="monitor monitor-left"><i/><i/><i/></span><span className="monitor monitor-right"><i/><i/></span><span className="keyboard"/><span className="desk-lamp"/><span className="desk-leg leg-a"/><span className="desk-leg leg-b"/></span><PixelWorker agent={agent}/><span className="desk-label">{agent.desk}</span>
  </button>;
}

function WorkDagPanel() {
  return <article className="mission-panel"><div className="panel-heading"><div><span>OPTIONAL WORK DAG</span><h2>Research / Review / Optimization / Release</h2></div><b>1 OPTIONAL DECISION · 3 RELEASE GATES</b></div><ol>{pipeline.map(([number,title,description])=><li key={`${number}-${title}`} className={number.startsWith('G')?'gate':title==='DATA + BACKTEST'||title==='OPTIMIZE'?'optional':'done'}><span>{number}</span><div><b>{title}</b><small>{description}</small></div><em>{number.startsWith('G')?'APPROVAL':title==='DATA + BACKTEST'||title==='OPTIMIZE'?'OPTIONAL':'READY'}</em></li>)}</ol></article>;
}

function WallProgressPanel({order}:{order:WorkOrder|null}) {
  const progress=getWorkflowProgress(order);
  return <aside className={`wall-progress progress-${progress.tone}`} aria-label="Current workflow progress" aria-live="polite"><header><span>CURRENT STAGE</span><em>{progress.percent}%</em></header><b>{progress.label}</b><small>{order?`${order.artifact_id??order.id} · ${order.status.replaceAll('_',' ')}`:'WAITING FOR A WORK ORDER'}</small><i><em style={{width:`${progress.percent}%`}}/></i></aside>;
}

function BacktestFiles({orders,onOpenChart}:{orders:WorkOrder[];onOpenChart:(artifact:ReportArtifact)=>void}) {
  if(!orders.length)return <section className="backtest-files empty"><span>BACKTEST FILES</span><p>No completed report packages yet.</p></section>;
  return <section className="backtest-files"><div className="backtest-file-heading"><span>BACKTEST FILES</span><b>{String(orders.length).padStart(2,'0')}</b></div><div className="backtest-file-list">{orders.map((order)=>{const file=order.backtest_file!;const primary=file.artifacts.filter((artifact)=>artifact.kind!=='chart');const charts=file.artifacts.filter((artifact)=>artifact.kind==='chart');return <article key={order.id}><div><b>{file.title}</b><em>{file.status}</em></div><small>{file.candle_count?`${file.candle_count.toLocaleString()} CANDLES`:'CANDLE COUNT N/A'} · {order.id}</small><nav>{primary.map((artifact)=><a key={artifact.url} href={`${gatewayBase}${artifact.url}`} target="_blank" rel="noreferrer">{artifact.label}</a>)}</nav>{charts.length>0&&<details><summary>{charts.length} CHARTS</summary><nav>{charts.map((artifact)=><button key={artifact.url} type="button" onClick={()=>onOpenChart(artifact)}>{artifact.label}</button>)}</nav></details>}</article>;})}</div></section>;
}

const approvalLabels:Record<ApprovalKey,string> = {implementation:'IMPLEMENTATION',optimization:'OPTIMIZATION PLAN',backtest:'BACKTEST',live:'LIVE'};
function ApprovalPanel({order,note,busy,enabled,onNote,onDecide}:{order:WorkOrder;note:string;busy:boolean;enabled:boolean;onNote:(value:string)=>void;onDecide:(gate:ApprovalKey,approved:boolean)=>void}) {
  const required:ApprovalKey|null = order.status==='awaiting_implementation_approval'?'implementation':order.status==='awaiting_optimization_approval'?'optimization':order.status==='awaiting_backtest_approval'?'backtest':order.status==='awaiting_live_approval'?'live':null;
  return <section className="approval-panel"><div className="approval-heading"><span>RESEARCH DECISIONS & RELEASE GATES</span><b>{required?`${approvalLabels[required]} DECISION REQUIRED`:'NO PENDING DECISION'}</b></div><div className="approval-gates">{(['implementation','optimization','backtest','live'] as ApprovalKey[]).map((gate)=>{const decision=order.approvals?.[gate];const skipped=(gate==='backtest'&&order.backtest_required===false)||(gate==='optimization'&&!order.optimization_required);return <article key={gate} className={decision?.approved?'approved':decision?'rejected':required===gate?'active':skipped?'skipped':'pending'}><small>{approvalLabels[gate]}</small><strong>{skipped?'SKIPPED':decision?.approved?'APPROVED':decision?'SKIPPED':required===gate?'WAITING':'LOCKED'}</strong>{decision&&<em>{decision.note}</em>}</article>;})}</div>{required&&<><label htmlFor="approval-note">DECISION NOTE</label><textarea id="approval-note" value={note} onChange={(event)=>onNote(event.target.value)} placeholder="Record why this decision is approved or skipped…" disabled={!enabled||busy}/><div className="approval-actions"><button type="button" className="reject" disabled={!enabled||busy||!note.trim()} onClick={()=>onDecide(required,false)}>{required==='optimization'?'KEEP CURRENT VERSION':'RETURN'}</button><button type="button" className="approve" disabled={!enabled||busy||!note.trim()} onClick={()=>onDecide(required,true)}>{required==='optimization'?'APPROVE REVISION':'APPROVE'}</button></div></>}</section>;
}

export default function Home() {
  const [selectedId,setSelectedId] = useState<AgentId>('manager');
  const [runtime,setRuntime] = useState<Record<AgentId,AgentRuntime>>(initialRuntime);
  const [chatInput,setChatInput] = useState('');
  const [attachments,setAttachments] = useState<File[]>([]);
  const [specKind,setSpecKind] = useState<'signal'|'strategy'>('strategy');
  const [specBacktestRequired,setSpecBacktestRequired] = useState(true);
  const [specError,setSpecError] = useState('');
  const [templateStatus,setTemplateStatus] = useState<'idle'|'downloading'|'downloaded'|'error'>('idle');
  const [dispatching,setDispatching] = useState(false);
  const [approvalBusy,setApprovalBusy] = useState(false);
  const [approvalNote,setApprovalNote] = useState('');
  const [connection,setConnection] = useState<ConnectionState>('CONNECTING');
  const [codexHealth,setCodexHealth] = useState<CodexHealth|null>(null);
  const [codexLoginBusy,setCodexLoginBusy] = useState(false);
  const [controlMode,setControlMode] = useState<'detecting'|'local'|'public'>('detecting');
  const [snapshot,setSnapshot] = useState<ApiSnapshot|null>(null);
  const [previewArtifact,setPreviewArtifact] = useState<ReportArtifact|null>(null);
  const [previewError,setPreviewError] = useState('');
  const [handoff,setHandoff] = useState<Handoff|null>(null);
  const [handoffMotion,setHandoffMotion] = useState<HandoffMotion|null>(null);
  const [handoffPhase,setHandoffPhase] = useState<HandoffPhase>('outbound');
  const [messages,setMessages] = useState<ChatMessage[]>([{id:'welcome',author:'MANAGER',text:'正在連接本機 Cha!n Studio Gateway。未連線前，網頁不會模擬任何工作結果。'}]);
  const sceneRef = useRef<HTMLDivElement|null>(null);
  const handoffQueue = useRef<Handoff[]>([]);
  const previousSnapshot = useRef<ApiSnapshot|null>(null);
  const latestSnapshotAt = useRef(0);
  const messageId = useRef(1);
  const selected = useMemo(()=>agents.find((agent)=>agent.id===selectedId)??agents[0],[selectedId]);
  const attachedSpec = useMemo(()=>attachments.find((file)=>file.name.toLowerCase()==='spec.md')??null,[attachments]);
  const isLocalControl=controlMode==='local';
  const selectedCurrent = runtime[selected.id].task||selected.current;
  const backtestOrders = useMemo(()=>snapshot?.work_orders.filter((order)=>order.backtest_file).slice().reverse()??[],[snapshot]);
  const currentWorkflowOrder = useMemo(()=>{const orders=snapshot?.work_orders??[];return orders.slice().reverse().find((order)=>!['completed','cancelled'].includes(order.status))??orders.at(-1)??null;},[snapshot]);
  const selectedOrder = useMemo(()=>snapshot?.work_orders.slice().reverse().find((order)=>{
    if(selected.id==='manager')return ['awaiting_implementation_approval','awaiting_optimization_approval','awaiting_backtest_approval','awaiting_live_approval'].includes(order.status);
    if(selected.id==='backtest')return Boolean(order.backtest_file)||order.stage==='backtest'||order.stage==='backtest_approval';
    if(selected.id==='review')return order.stage==='review_implementation'||order.stage==='review_backtest'||['awaiting_implementation_approval','awaiting_backtest_approval'].includes(order.status);
    if(selected.id==='optimize')return order.stage==='optimize'||order.stage==='optimization_approval';
    if(selected.id==='signal')return order.stage==='ready_for_signal';
    return order.agent===selected.id||order.stage===selected.id;
  }),[snapshot,selected.id]);
  const playHandoff = useCallback((from:AgentId,to:AgentId,label:string)=>{
    if(from===to)return;
    const next:Handoff={from,to,label,key:Date.now()+Math.random()};
    setHandoff((current)=>{
      if(current){handoffQueue.current.push(next);return current;}
      return next;
    });
  },[]);
  const applySnapshot = useCallback((next:ApiSnapshot)=>{
    const generatedAt=Date.parse(next.generated_at);
    if(Number.isFinite(generatedAt)&&generatedAt<latestSnapshotAt.current)return;
    if(Number.isFinite(generatedAt))latestSnapshotAt.current=generatedAt;
    const mergedAgents={...initialRuntime,...next.agents} as ApiSnapshot['agents'];
    const authoritativeSignal:AgentRuntime&{executable:boolean;execution:string}={
      ...mergedAgents.signal,
      status:next.signal_runtime.running?'RUNNING':'STOPPED',
      task:next.signal_runtime.detail,
      progress:next.signal_runtime.running?100:0,
    };
    const normalizedNext:ApiSnapshot={...next,agents:{...mergedAgents,signal:authoritativeSignal}};
    const previous=previousSnapshot.current;
    if(previous){
      const previousOrders=new Map(previous.work_orders.map((order)=>[order.id,order]));
      for(const order of normalizedNext.work_orders){
        const prior=previousOrders.get(order.id);
        if(!prior)continue;
        const ownerForStage=(stage:string|undefined):AgentId|undefined=>stage==='review_implementation'||stage==='review_backtest'?'review':stage?.endsWith('_approval')?'manager':agents.some((agent)=>agent.id===stage)?stage as AgentId:undefined;
        const priorStage=ownerForStage(prior.stage);
        const nextStage=ownerForStage(order.stage);
        if(priorStage&&nextStage&&priorStage!==nextStage){
          playHandoff(priorStage,nextStage,'WORK TRANSFER');
        }else if(!prior.status.startsWith('awaiting_')&&order.status.startsWith('awaiting_')){
          playHandoff(priorStage??order.agent,'manager','RESULT DELIVERY');
        }else if(prior.status==='working'&&order.status==='completed'&&nextStage){
          playHandoff(nextStage,'manager','TASK COMPLETE');
        }
      }
    }
    previousSnapshot.current=normalizedNext;
    setSnapshot(normalizedNext);setRuntime(normalizedNext.agents);setMessages(normalizedNext.messages.length?normalizedNext.messages:[{id:'ready',author:'MANAGER',text:'Manager Agent 已連上本機系統，可新增 Signal、建立交易策略、索取資料或管理工作流。'}]);setConnection('CONNECTED');
  },[playHandoff]);
  const refresh = useCallback(async()=>{
    if(controlMode==='public'){setConnection('OFFLINE');return;}
    try {const response=await fetch(`${gatewayBase}/api/v1/studio`,{cache:'no-store',signal:AbortSignal.timeout(2500)});if(!response.ok)throw new Error();applySnapshot(await response.json() as ApiSnapshot);}
    catch {setConnection('OFFLINE');setRuntime({...initialRuntime,manager:{status:'STOPPED',task:'Local Studio Gateway offline',progress:0},signal:{status:'STOPPED',task:'Gateway offline; runtime state unknown',progress:0}});}
  },[applySnapshot,controlMode]);
  const refreshCodexHealth = useCallback(async()=>{
    if(controlMode!=='local')return;
    try{const response=await fetch(`${gatewayBase}/api/v1/codex/health`,{cache:'no-store',signal:AbortSignal.timeout(20000)});if(!response.ok)throw new Error();setCodexHealth(await response.json() as CodexHealth);}
    catch{setCodexHealth(null);}
  },[controlMode]);
  const startCodexLogin = useCallback(async()=>{
    if(controlMode!=='local'||codexLoginBusy)return;
    setCodexLoginBusy(true);
    try{const response=await fetch(`${gatewayBase}/api/v1/codex/login`,{method:'POST',headers:{'X-Chain-Client':'quant-studio-v1'},signal:AbortSignal.timeout(10000)});if(!response.ok)throw new Error(await response.text());setMessages((current)=>[...current,{id:`codex-login-${messageId.current++}`,author:'MANAGER',text:'Codex device login started. Complete the browser sign-in, then select RECHECK.'}]);window.setTimeout(()=>void refreshCodexHealth(),2500);}
    catch(error){setMessages((current)=>[...current,{id:`codex-login-error-${messageId.current++}`,author:'MANAGER',text:`Codex login could not start: ${error instanceof Error?error.message:'Unknown error'}`}]);}
    finally{setCodexLoginBusy(false);}
  },[codexLoginBusy,controlMode,refreshCodexHealth]);
  useEffect(()=>{const host=window.location.hostname;const tauri='__TAURI_INTERNALS__' in window;setControlMode(tauri||host==='localhost'||host==='127.0.0.1'?'local':'public');},[]);
  useEffect(()=>{if(controlMode==='detecting')return;if(controlMode==='public'){setMessages([{id:'public',author:'MANAGER',text:'Public dashboard is display-only. Open the CHA!N desktop app or localhost workspace for commands and approvals.'}]);return;}void refresh();const poller=window.setInterval(()=>void refresh(),3000);const wake=()=>void refresh();window.addEventListener('focus',wake);document.addEventListener('visibilitychange',wake);return()=>{window.clearInterval(poller);window.removeEventListener('focus',wake);document.removeEventListener('visibilitychange',wake);};},[controlMode,refresh]);
  useEffect(()=>{if(controlMode!=='local')return;void refreshCodexHealth();const poller=window.setInterval(()=>void refreshCodexHealth(),60000);return()=>window.clearInterval(poller);},[controlMode,refreshCodexHealth]);
  useLayoutEffect(()=>{
    if(!handoff){setHandoffMotion(null);setHandoffPhase('outbound');return;}
    const scene=sceneRef.current;
    const fromWorker=scene?.querySelector<HTMLElement>(`[data-agent-id="${handoff.from}"] .pixel-worker`);
    const toWorker=scene?.querySelector<HTMLElement>(`[data-agent-id="${handoff.to}"] .pixel-worker`);
    if(!scene||!fromWorker||!toWorker){setHandoffMotion(null);return;}
    const sceneBox=scene.getBoundingClientRect();
    const floorPoint=(worker:HTMLElement)=>{const box=worker.getBoundingClientRect();return{x:box.left-sceneBox.left+box.width/2-29,y:box.bottom-sceneBox.top-74};};
    const start=floorPoint(fromWorker);const end=floorPoint(toWorker);
    const distance=Math.hypot(end.x-start.x,end.y-start.y);
    setHandoffMotion({startX:start.x,startY:start.y,endX:end.x,endY:end.y,duration:Math.round(Math.min(1800,Math.max(950,650+distance*1.6)))});
    setHandoffPhase('outbound');
  },[handoff]);
  useEffect(()=>{
    if(!handoff||!handoffMotion)return;
    const timer=window.setTimeout(()=>{
      const nextPhase=advanceHandoffPhase(handoffPhase);
      if(nextPhase){setHandoffPhase(nextPhase);return;}
      setHandoff(handoffQueue.current.shift()??null);
    },handoffMotion.duration+120);
    return()=>window.clearTimeout(timer);
  },[handoff,handoffMotion,handoffPhase]);
  useEffect(()=>{if(!previewArtifact)return;const close=(event:KeyboardEvent)=>{if(event.key==='Escape')setPreviewArtifact(null);};window.addEventListener('keydown',close);return()=>window.removeEventListener('keydown',close);},[previewArtifact]);
  const dispatchInstruction=async(event:FormEvent<HTMLFormElement>)=>{
    event.preventDefault();const instruction=chatInput.trim();if((!instruction&&!attachedSpec)||dispatching||connection!=='CONNECTED'||!isLocalControl)return;
    setChatInput('');setSpecError('');setDispatching(true);setMessages((current)=>[...current,{id:`local-${messageId.current++}`,author:'YOU',text:attachments.length?`${instruction||'Upload spec.md'} · ${attachments.map((file)=>file.name).join(', ')}`:instruction}]);setRuntime((current)=>({...current,manager:{status:'WORKING',task:attachments.length?'Reading message attachments':'Understanding intent with Codex',progress:45}}));
    try {let response:Response;if(attachments.length){const form=new FormData();form.append('message',instruction);form.append('artifact_kind',attachedSpec?specKind:'');form.append('backtest_required',attachedSpec?String(specBacktestRequired):'');for(const file of attachments)form.append('attachment',file,file.name);response=await fetch(`${gatewayBase}/api/v1/manager/attachments`,{method:'POST',headers:{'X-Chain-Client':'quant-studio-v1'},body:form,signal:AbortSignal.timeout(60000)});}else{response=await fetch(`${gatewayBase}/api/v1/manager/message`,{method:'POST',headers:{'Content-Type':'application/json','X-Chain-Client':'quant-studio-v1'},body:JSON.stringify({message:instruction}),signal:AbortSignal.timeout(60000)});}if(!response.ok){const detail=await response.text();setMessages((current)=>[...current,{id:`error-${messageId.current++}`,author:'MANAGER',text:`指令未執行：${detail}`}]);await refresh();return;}const result=await response.json() as ManagerResponse;setAttachments([]);applySnapshot(result.snapshot);if(result.handoff){playHandoff('manager',result.handoff,'TASK DISPATCH');}}
    catch(error){setConnection('OFFLINE');setMessages((current)=>[...current,{id:`error-${messageId.current++}`,author:'MANAGER',text:`指令未執行：本機 Studio Gateway 無法連線。${error instanceof Error?` ${error.message}`:''}`}]);}
    finally{setDispatching(false);}
  };
  const downloadSpecTemplate=async()=>{
    if(templateStatus==='downloading')return;
    setTemplateStatus('downloading');
    try{
      const response=await fetch('/spec.template.md',{cache:'no-store'});
      if(!response.ok)throw new Error(`HTTP ${response.status}`);
      const blob=await response.blob();
      const objectUrl=URL.createObjectURL(blob);
      const download=document.createElement('a');
      download.href=objectUrl;
      download.download='spec.md';
      download.style.display='none';
      document.body.appendChild(download);
      download.click();
      download.remove();
      window.setTimeout(()=>URL.revokeObjectURL(objectUrl),1000);
      setTemplateStatus('downloaded');
    }catch{
      setTemplateStatus('error');
    }
  };
  const selectAttachments=(picked:File[])=>{
    const allowed=/\.(md|txt|png|jpe?g|webp)$/i;
    if(picked.length>6){setSpecError('You can attach up to 6 files.');return;}
    const names=picked.map((file)=>file.name.toLowerCase());
    if(new Set(names).size!==names.length){setSpecError('Attachment filenames must be unique.');return;}
    if(names.filter((name)=>name==='spec.md').length>1){setSpecError('Attach only one spec.md.');return;}
    for(const file of picked){
      if(!allowed.test(file.name)){setSpecError('Use .md, .txt, .png, .jpg, .jpeg, or .webp files.');return;}
      const image=/\.(png|jpe?g|webp)$/i.test(file.name);
      const limit=file.name.toLowerCase()==='spec.md'?64*1024:image?8*1024*1024:512*1024;
      if(file.size>limit){setSpecError(`${file.name} is too large.`);return;}
    }
    setAttachments(picked);setSpecError('');
  };
  const decideApproval=async(gate:ApprovalKey,approved:boolean)=>{
    if(!selectedOrder||!isLocalControl||connection!=='CONNECTED'||approvalBusy||!approvalNote.trim())return;
    setApprovalBusy(true);
    try{const response=await fetch(`${gatewayBase}/api/v1/work-orders/${encodeURIComponent(selectedOrder.id)}/approval`,{method:'POST',headers:{'Content-Type':'application/json','X-Chain-Client':'quant-studio-v1'},body:JSON.stringify({gate:`${gate}_approval`,approved,note:approvalNote.trim()}),signal:AbortSignal.timeout(15000)});if(!response.ok)throw new Error(await response.text());const result=await response.json() as {snapshot:ApiSnapshot};setApprovalNote('');applySnapshot(result.snapshot);const destination:AgentId=gate==='implementation'?(selectedOrder.backtest_required===false?'review':'backtest'):gate==='optimization'?(approved?'strategy':'manager'):gate==='backtest'?'review':'signal';if(destination!=='manager')playHandoff('manager',destination,approved?'APPROVAL GRANTED':'REVISION REQUEST');}
    catch(error){setMessages((current)=>[...current,{id:`approval-error-${messageId.current++}`,author:'MANAGER',text:`批准未送出：${error instanceof Error?error.message:'Unknown error'}`}]);}
    finally{setApprovalBusy(false);}
  };
  const liveFeed=(snapshot?.messages??[]).slice(-6).reverse().map((item)=>({time:new Date(item.at).toLocaleTimeString('zh-TW',{hour:'2-digit',minute:'2-digit'}),source:item.author,text:item.text,tone:item.author==='MANAGER'?'cyan':'mint'}));
  const liveRegistry=snapshot?.live_registry??[];
  const runningRegistry=liveRegistry.filter((entry)=>entry.running);
  const handoffAgent=handoff?agents.find((agent)=>agent.id===handoff.from)??null:null;
  return <main className="game-shell">
    <header className="topbar"><div className="game-brand"><span className="brand-candle" aria-label="Green candlestick exclamation mark"><i/><b/><em/></span><div><h1>Cha<span className="brand-bang">!</span>n</h1><p>QUANT STUDIO</p></div></div><div className="top-stats"><div><span>CONTROL SURFACE</span><b className={isLocalControl?'online':'locked'}>{controlMode==='public'?'READ ONLY':controlMode==='local'?'LOCAL':'CHECKING'}</b></div><div><span>SIGNAL RUNTIME</span><b className={connection==='CONNECTED'&&runtime.signal.status==='RUNNING'?'online':'offline'}>{connection==='CONNECTED'?runtime.signal.status:'UNKNOWN'}</b></div><div><span>TRADING DESK</span><b className="locked">LOCKED</b></div></div></header>
    <section className="command-layout"><div className="office-window" aria-label="Cha!n virtual quant studio"><div className="office-hud"><div><span className="hud-live"/> CHA!N · QUANT STUDIO</div><div>{isLocalControl?'DESKTOP / LOCAL CONTROL':'PUBLIC READ-ONLY'} <span className="hud-divider">/</span> TAIPEI</div></div><div ref={sceneRef} className="office-scene"><div className="window-bank" aria-hidden="true"><span/><span/><span/><span/></div><div className="wall-sign" aria-hidden="true"><b>CHA<span className="brand-bang">!</span>N</b><small>RESEARCH · VERIFY · SIGNAL</small></div><WallProgressPanel order={currentWorkflowOrder}/><div className="plant plant-a" aria-hidden="true"><i/><i/><i/></div><div className="plant plant-b" aria-hidden="true"><i/><i/><i/></div><div className="coffee-table" aria-hidden="true"><span>+</span></div><div className="floor-grid" aria-hidden="true"/>{agents.map((agent)=><Workstation key={agent.id} agent={agent} runtime={runtime[agent.id]} selected={selected.id===agent.id} departing={handoff?.from===agent.id&&Boolean(handoffMotion)} onSelect={()=>setSelectedId(agent.id)}/>)}{handoff&&handoffAgent&&<div key={handoff.key} className={`handoff-run ${handoffMotion?'is-ready':''} is-${handoffPhase}`} style={handoffMotion?{'--handoff-start-x':`${handoffMotion.startX}px`,'--handoff-start-y':`${handoffMotion.startY}px`,'--handoff-end-x':`${handoffMotion.endX}px`,'--handoff-end-y':`${handoffMotion.endY}px`,'--handoff-duration':`${handoffMotion.duration}ms`,'--agent-accent':handoffAgent.accent} as CSSProperties:undefined} aria-live="polite" aria-label={handoffPhase==='returning'?`${handoffAgent.name} returns to ${handoffAgent.desk}`:`${handoffAgent.name} walks to ${agents.find((agent)=>agent.id===handoff.to)?.name??handoff.to}: ${handoff.label}`}><PixelWorker agent={handoffAgent} moving carryingPackage={carriesHandoffPackage(handoffPhase)}/><small>{handoffPhase==='returning'?`${handoff.from.toUpperCase()} ← ${handoff.to.toUpperCase()} · RETURN`:`${handoff.from.toUpperCase()} → ${handoff.to.toUpperCase()}`}</small></div>}<div className="command-table" aria-hidden="true"><span>MANAGER<br/>DAG ROUTER</span><i/><i/><i/><i/><i/><i/></div><aside className="strategy-display" aria-label="System signal and strategy registry"><header><span>STRATEGY TV</span><b className={runningRegistry.length?'is-live':'is-idle'}>{runningRegistry.length?'LIVE':'STANDBY'}</b></header><div className="strategy-registry">{liveRegistry.length?liveRegistry.map((entry)=><article key={`${entry.kind}-${entry.id}`} className={entry.running?'is-running':'is-inactive'}><span>{entry.kind.toUpperCase()}</span><b>{entry.id}</b><small>{entry.version?`v${entry.version} · `:''}{entry.status}</small></article>):<p>NO REGISTERED ARTIFACTS</p>}</div><footer><span>{runningRegistry.length?`${runningRegistry.length} ACTIVE`:'NO ACTIVE ARTIFACT'}</span><small>{liveRegistry.length} REGISTERED</small></footer></aside></div><WorkDagPanel/></div>
      <aside className="game-sidebar"><div className="sidebar-title"><div><span>EMPLOYEE FILE</span><h2>Agent Profile</h2></div><b>{selected.number} / 08</b></div><article className="selected-agent" style={{'--agent-accent':selected.accent} as CSSProperties} aria-live="polite"><div className="portrait"><PixelWorker agent={selected}/></div><div className="selected-copy"><p>{selected.role}</p><h3>{selected.name}</h3><span className={`status-pill status-${runtime[selected.id].status}`}>{runtime[selected.id].status}</span></div></article>
        {selected.id==='manager'?<section className="manager-chat" aria-label="Chat with Manager Agent">
          <div className={`gateway-state gateway-${connection.toLowerCase()}`}><b>{isLocalControl?connection:'READ ONLY'}</b><span>{isLocalControl?(connection==='CONNECTED'?'Natural-language requests are semantically routed by Codex.':'Start qsa-studio on this computer to connect.'):'Commands and approvals are disabled on the public website.'}</span>{isLocalControl&&<button type="button" onClick={()=>void refresh()}>RETRY</button>}</div>
          <div className="chat-log" aria-live="polite">{messages.map((message,index)=><article key={`${message.id}-${message.at??'local'}-${index}`} className={`chat-${message.author.toLowerCase()}`}><b>{message.author}</b><p>{message.text}</p></article>)}</div>
          <form onSubmit={dispatchInstruction}>
            <label htmlFor="manager-message">MESSAGE MANAGER AGENT</label>
            <div className="manager-composer">
              <textarea id="manager-message" value={chatInput} onChange={(event)=>setChatInput(event.target.value)} placeholder={isLocalControl?'Describe the outcome and add optional context files…':'Public dashboard is read-only.'} rows={4} disabled={!isLocalControl||dispatching||connection!=='CONNECTED'}/>
              <div className="composer-tools">
                <input key={attachments.map((file)=>`${file.name}-${file.lastModified}`).join('|')||'empty-attachments'} className="spec-file-input" id="manager-attachments" type="file" accept=".md,.txt,.png,.jpg,.jpeg,.webp,text/markdown,text/plain,image/png,image/jpeg,image/webp" multiple disabled={!isLocalControl||dispatching||connection!=='CONNECTED'} onChange={(event)=>selectAttachments(Array.from(event.target.files??[]))}/>
                <label className="spec-file-button" htmlFor="manager-attachments">ATTACH FILES</label>
                <button className="template-button" type="button" onClick={()=>void downloadSpecTemplate()} disabled={templateStatus==='downloading'}>{templateStatus==='downloading'?'DOWNLOADING…':'GET SPEC TEMPLATE'}</button>
                <span>{attachments.length?`${attachments.length} FILE${attachments.length===1?'':'S'}`:'NO FILES'}</span>
              </div>
              {attachments.length>0&&<div className="attachment-chips">{attachments.map((file)=><button key={`${file.name}-${file.lastModified}`} type="button" onClick={()=>setAttachments((current)=>current.filter((item)=>item!==file))} title={`Remove ${file.name}`}><b>{file.name}</b><span>{(file.size/1024).toFixed(1)} KB</span><i>×</i></button>)}</div>}
              {attachedSpec&&<div className="spec-options"><label className="spec-field" htmlFor="manager-spec-kind">ARTIFACT TYPE<select id="manager-spec-kind" value={specKind} onChange={(event)=>setSpecKind(event.target.value as 'signal'|'strategy')} disabled={!isLocalControl||dispatching}><option value="strategy">Trading Strategy</option><option value="signal">Signal Definition</option></select></label><label className="spec-field" htmlFor="manager-backtest">BACKTEST REQUIRED?<select id="manager-backtest" value={specBacktestRequired?'yes':'no'} onChange={(event)=>setSpecBacktestRequired(event.target.value==='yes')} disabled={!isLocalControl||dispatching}><option value="yes">Yes — report & charts</option><option value="no">No — implementation tests only</option></select></label></div>}
              {specError&&<small className="spec-error">{specError}</small>}
              {templateStatus==='downloaded'&&<small className="spec-template-status" role="status">TEMPLATE DOWNLOADED AS SPEC.MD</small>}
              {templateStatus==='error'&&<small className="spec-error" role="alert">TEMPLATE DOWNLOAD FAILED — PLEASE TRY AGAIN</small>}
            </div>
            <button type="submit" disabled={!isLocalControl||(!chatInput.trim()&&!attachedSpec)||dispatching||connection!=='CONNECTED'}>{dispatching?(attachments.length?'UPLOADING & ROUTING…':'UNDERSTANDING…'):(attachments.length?'SEND WITH FILES':'SEND INSTRUCTION')}</button>
          </form>
          <div className="manager-scope" aria-label="Manager Agent permission scope"><span>SEMANTIC ROUTER · HARD SAFETY GATE</span><div><b>ADD SIGNAL</b><b>MODIFY SIGNAL</b><b>ADD STRATEGY</b><b>MODIFY STRATEGY</b><b>REQUEST AGENT DATA</b><b>MANAGE WORKFLOW</b></div><small>DENIED: CORE CODE · OFF-TOPIC · TRADING</small></div>
          <section className="operations-card"><span>CONTROL PLANE</span><div><b>CODEX SDK</b><em className={codexHealth?.ready?'ok':'warn'}>{codexHealth?.ready?'READY':codexHealth?'LOGIN REQUIRED':'CHECKING'}</em></div>{isLocalControl&&codexHealth&&!codexHealth.ready&&<aside className="codex-onboarding"><p>Sign in once on this device. Credentials stay in this operating-system user profile and are never copied into the project.</p><button type="button" onClick={()=>void startCodexLogin()} disabled={codexLoginBusy||codexHealth.login_in_progress}>{codexLoginBusy||codexHealth.login_in_progress?'LOGIN IN PROGRESS…':'SIGN IN TO CODEX'}</button><button type="button" onClick={()=>void refreshCodexHealth()}>RECHECK</button></aside>}<div><b>CODEX CIRCUIT</b><em className={snapshot?.codex_circuit?.state==='open'?'warn':'ok'}>{snapshot?.codex_circuit?.state?.toUpperCase()??'UNKNOWN'}</em></div><div><b>USER ACTION</b><em className={snapshot?.user_blockers?.length?'warn':'ok'}>{snapshot?.user_blockers?.length?'REQUIRED':'CLEAR'}</em></div><div><b>DEAD LETTERS</b><em>{snapshot?.dead_letters?.length??0}</em></div><div><b>ARTIFACTS</b><em>{snapshot?.artifact_registry?.length??0}</em></div><div><b>DATASETS</b><em>{snapshot?.data_service?.datasets?.length??0}</em></div>{snapshot?.user_blockers?.map((blocker)=><small key={blocker.code} className="user-blocker">{blocker.detail}</small>)}{snapshot?.codex_circuit?.open_until&&<small>Next provider retry: {new Date(snapshot.codex_circuit.open_until).toLocaleString('zh-TW')}</small>}</section>
          {selectedOrder&&<ApprovalPanel order={selectedOrder} note={approvalNote} busy={approvalBusy} enabled={isLocalControl&&connection==='CONNECTED'} onNote={setApprovalNote} onDecide={(gate,approved)=>void decideApproval(gate,approved)}/>}
          <p className="chat-disclosure">Codex understands intent and selects an allowlisted action. Signal means a neutral condition; Strategy means directional entry, exit, and risk. Backtest is optional only when explicitly selected; implementation tests and user review always remain required.</p>
        </section>:<>
          <dl className="task-file"><div><dt>CURRENT TASK</dt><dd>{selectedCurrent}</dd></div><div><dt>HANDOFF PACKAGE</dt><dd>{selected.handoff}</dd></div></dl>
          {selected.id==='backtest'?<BacktestFiles orders={backtestOrders} onOpenChart={(artifact)=>{setPreviewError('');setPreviewArtifact(artifact);}}/>:selectedOrder&&<section className="work-order-card"><span>LIVE {selectedOrder.artifact_kind?.toUpperCase()??'WORK'} ORDER</span><b>{selectedOrder.id}</b><div><em>{selectedOrder.stage??selectedOrder.agent}</em><strong>{selectedOrder.status}</strong></div>{selectedOrder.backtest_required===false&&<small>BACKTEST SKIPPED BY USER</small>}{selectedOrder.dag&&<details><summary>VIEW WORK DAG</summary><pre>{selectedOrder.dag.nodes.map((node)=>`${node.node_id} [${node.owner}] ← ${node.requires.join(', ')||'START'}`).join('\n')}</pre></details>}{selectedOrder.results&&<details><summary>VIEW RESULT</summary><pre>{Object.entries(selectedOrder.results).map(([role,result])=>`${role.toUpperCase()}\n${result}`).join('\n\n')}</pre></details>}</section>}
        </>}
        <div className="safety-card"><span>SAFETY PROTOCOL</span><b>Order execution is disabled</b><p>SIGNAL is advisory only. Trading Agent cannot place orders.</p></div></aside></section>
    <section className="lower-deck"><aside className="feed-panel"><div className="panel-heading"><div><span>STUDIO COMMS</span><h2>Current Progress</h2></div><b>{String(liveFeed.length).padStart(2,'0')} MSG</b></div><div className="feed-list">{liveFeed.length?liveFeed.map((item,index)=><article key={`${item.time}-${index}`} className={`feed-${item.tone}`}><time>{item.time}</time><div><b>{item.source}</b><p>{item.text}</p></div></article>):<article className="feed-gray"><time>NOW</time><div><b>SYSTEM</b><p>{isLocalControl?(connection==='CONNECTED'?'No work orders yet.':'Waiting for local Gateway connection.'):'Public read-only dashboard.'}</p></div></article>}</div></aside></section>
    {previewArtifact&&<div className="artifact-modal" role="presentation" onMouseDown={(event)=>{if(event.target===event.currentTarget)setPreviewArtifact(null);}}><section role="dialog" aria-modal="true" aria-labelledby="artifact-preview-title"><header><div><span>BACKTEST CHART</span><h2 id="artifact-preview-title">{previewArtifact.label}</h2><small>{previewArtifact.name}</small></div><button type="button" onClick={()=>setPreviewArtifact(null)}>CLOSE</button></header>{previewError?<p className="artifact-preview-error">{previewError}</p>:<div className="artifact-preview-body"><Image unoptimized width={920} height={420} src={`${gatewayBase}${previewArtifact.url}`} alt={`${previewArtifact.label} backtest chart`} onError={()=>setPreviewError('Chart could not be loaded from the local Studio Gateway.')}/></div>}</section></div>}
    <footer className="game-footer"><span>CHA!N SYSTEM BUILD 014</span><p>MANAGER → REVIEW → OPTIMIZE → USER DECISION → RELEASE</p><b>{connection!=='CONNECTED'?'SIGNAL UNKNOWN':snapshot?.signal_runtime.running?`SIGNAL PID ${snapshot.signal_runtime.pid}`:'SIGNAL STOPPED'}</b></footer>
  </main>;
}
