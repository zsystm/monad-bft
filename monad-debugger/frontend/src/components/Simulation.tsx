import { Component, createMemo, createSignal, onCleanup, Show } from 'solid-js';
import { createStore, reconcile } from "solid-js/store"
import { SimulationDocument } from '../generated/graphql';
import { Simulation } from '../wasm'
import EventLog from './EventLog';
import Graph from './Graph'
import QueryEditor from './QueryEditor';
import { throttle } from '@solid-primitives/scheduled';

const maxTick = 2000;

const Sim: Component = () => {
    const simulation = new Simulation();
    onCleanup(() => {
        simulation.free();
    });
    const fetchSimulationData = () => simulation.fetchUnchecked(SimulationDocument);

    const [simData, setSimData] = createStore(fetchSimulationData())
    const setTick = (tick: number) => {
        simulation.setTick(tick);
        setSimData(reconcile(fetchSimulationData(), { merge: true, key: 'id' }));
    };

    const step = () => {
        simulation.step();
        setSimData(reconcile(fetchSimulationData(), { merge: true, key: 'id' }));
    };

    const tick = () => simData.currentTick;

    const simulationSignal = () => {
        const _ = tick();
        return simulation;
    };

    const [playing, setPlaying] = createSignal(false);
    const timer = setInterval(() => {
        if (playing()) {
            setTick((tick() + 1) % maxTick);
        }
    }, 30);
    onCleanup(() => clearInterval(timer));

    const [showEventLog, setShowEventLog] = createSignal(false);
    const toggleEventLog = () => setShowEventLog(!showEventLog())

    const [showQueryEditor, setShowQueryEditor] = createSignal(false);
    const toggleQueryEditor = () => setShowQueryEditor(!showQueryEditor())

    return (
        <>
            <input type="range" min="0" max={maxTick} value={tick()} onInput={throttle(e => setTick(parseInt(e.target.value)), 5)} />
            <div class="flex justify-between">
                <div class="min-w-40">
                    <span class="ml-2 mr-2">Current Tick: {tick()}</span>
                </div>
                <div>
                    <button class="border ml-2 mr-2" onClick={() => step()}>Step</button>
                    <Show
                        when={!playing()}
                        fallback={
                            <button class="border ml-2 mr-2" onClick={() => setPlaying(false)}>Stop</button>
                        }
                        >
                        <button class="border ml-2 mr-2" onClick={() => setPlaying(true)}>Play</button>
                    </Show>
                </div>
                <div>
                    <button class="border ml-2 mr-2" onClick={toggleQueryEditor}>Toggle Query Editor</button>
                    <button class="border ml-2 mr-2" onClick={toggleEventLog}>Toggle Event Log</button>
                </div>
            </div>
            <div class="grow flex flex-row min-h-0">
                <Show when={showQueryEditor()}>
                    <QueryEditor simulation={simulationSignal()} />
                </Show>
                <Graph simulation={simulationSignal()} />
                <Show when={showEventLog()}>
                    <EventLog simulation={simulationSignal()} />
                </Show>
            </div>
        </>
    )
};
export default Sim;