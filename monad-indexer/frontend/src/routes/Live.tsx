import { A } from '@solidjs/router';
import { Component, createMemo, createSignal, For, onCleanup, Show } from 'solid-js';

import nodeNames from '../nodes.json';

interface Block {
  block_id: string,
  timestamp: number,
  author: string,
  epoch: number,
  round: number,
  state_root: string,
  seq_num: number,
  beneficiary: string,
  randao_reveal: string,
  payload_id?: string,
  parent_block_id: string,
}

const REFRESH_INTERVAL = 5000;

const Live: Component = () => {
  const [blocks, setBlocks] = createSignal<Block[]>([]);
  const [refreshingIn, setRefreshingIn] = createSignal(Date.now());
  let skipPoll = false;
  const poll = () => {
    if (skipPoll) {
      return;
    }
    skipPoll = true;
    fetch('/api/v1/blocks').then(async response => {
      const blocks = await response.json();
      setBlocks(blocks)
    }).finally(() => {
      skipPoll = false;
    });
    setRefreshingIn(Date.now() + REFRESH_INTERVAL);
  };
  poll();
  const pollTimer = setInterval(poll, REFRESH_INTERVAL);
  onCleanup(() => clearInterval(pollTimer));

  const [currentTime, setCurrentTime] = createSignal(Date.now());
  const timeTimer = setInterval(() => {
    setCurrentTime(Date.now());
  }, 100);

  const secondsAgo = (block: Block): number => {
    const blockTime = new Date(block.timestamp);
    const now = currentTime();
    return Math.floor((now - block.timestamp) / 1000);
  }

  const latestBlock = () => {
    const latestBlock: Block | undefined = blocks()[0];
    return latestBlock;
  };

  const blocksAndSkipped = createMemo(() => {
    const latest = latestBlock();
    if (!latest) {
      return [];
    }

    let round = latest.round;

    const recentBlocks = blocks();
    const blocksAndSkipped = [];

    for (const block of recentBlocks) {
      while (round !== block.round) {
        blocksAndSkipped.push(null);
        round -= 1;
      }
      blocksAndSkipped.push(block);
      round -= 1;
    }

    return blocksAndSkipped;
  });

  return (
    <div>
      <div class="text-4xl">Refreshing in {Math.ceil((refreshingIn() - currentTime()) / 1000)} seconds</div>
      <For each={blocksAndSkipped()}>{(block, i) =>
        <Show when={block} fallback={
          // fallback
          <div>
            {latestBlock().round - i()} - Timeout
          </div>
        } keyed>{block => 
          <div>
            {block.round} - 
            <A href={`/block/${block.block_id}`}>
              {block.seq_num}
            </A>
            - {(nodeNames as any)[block.author.slice(2)] ?? block.author} - {secondsAgo(block)} seconds ago
          </div>
        }</Show>
      }</For>
    </div>
  );
};

export default Live;
