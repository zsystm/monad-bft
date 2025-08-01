import { Component } from "solid-js";
import { NodeFragment } from "../generated/graphql";

const Node: Component<{
    currentTick: number,
    node: NodeFragment,
    idx: number,
}> = (props) => {
    const node = () => props.node;
    const timeoutProgress = () => (props.currentTick - node().roundTimerStartedAt) / (node().roundTimerEndsAt - node().roundTimerStartedAt);
    const scaledTimeoutProgress = () => 1 - Math.exp(-1 * timeoutProgress());
    return (
        <div class="p-0.5 rounded-2xl" style={{
            '--progress': `${100 * timeoutProgress()}%`,
            background: `conic-gradient(from 0deg, rgba(255,0,0,${scaledTimeoutProgress()}) 0%, rgba(255,0,0,${scaledTimeoutProgress()}) var(--progress), transparent var(--progress))`
        }}>
            <div class="min-w-56 text-nowrap p-4 rounded-2xl bg-gray-100 border-2 border-black shadow-md">
                <div class="text-3xl center">
                    {/* Node {formatNodeId(node().id)} */}
                    Node {props.idx + 1}
                </div>
                <div>
                    Current round: {node().currentRound}
                </div>
                <div>
                    Latest finalized: {node().root}
                </div>
                <div>
                    Timeout progress: {Math.round(100 * timeoutProgress())}%
                </div>
            </div>
        </div>
    )
};

export default Node;
