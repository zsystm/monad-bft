import { Component } from "solid-js";
import { MessageFragment } from "../generated/graphql";

const Message: Component<{
    message: MessageFragment,
}> = (props) => {
    const message = () => props.message;

    if (message().__typename !== "GraphQLConsensusMessage") {
        return <div>Unknown message</div>
    }
    const round = () => message().round;

    const messageType = message().message;
    switch (messageType.__typename) {
        case "GraphQLProposal":
            const seqNum = messageType.seqNum;
            return (
                <div class="text-nowrap p-2 rounded-2xl bg-green-400/50 border border-black shadow-sm">
                    <div class="text-2xl">Proposal</div>
                    <div>round={round()}</div>
                    <div>seq_num={seqNum}</div>
                </div>
            );
        case "GraphQLVote":
            return (
                <div class="text-nowrap p-2 rounded-2xl bg-green-400/15 border border-black shadow-sm">
                    <div class="text-2xl">Vote</div>
                    <div>round={round()}</div>
                </div>
            );
        case "GraphQLTimeout":
            return (
                <div class="text-nowrap p-2 rounded-2xl bg-red-400/25 border border-black shadow-sm">
                    <div class="text-2xl">Timeout</div>
                    <div>round={round()}</div>
                </div>
            );
        case "GraphQLRoundRecovery":
            return (
                <div class="text-nowrap p-2 rounded-2xl border border-black shadow-sm">
                    <div class="text-2xl">RoundRecovery</div>
                    <div>round={round()}</div>
                </div>
            );
        case "GraphQLNoEndorsement":
            return (
                <div class="text-nowrap p-2 rounded-2xl border border-black shadow-sm">
                    <div class="text-2xl">NoEndorsement</div>
                    <div>round={round()}</div>
                </div>
            );
        default:
            assertUnreachable(messageType);
            return <div>Unknown message type</div>;
    }
};

function assertUnreachable(x?: never): never {
    throw new Error();
}

export default Message;
