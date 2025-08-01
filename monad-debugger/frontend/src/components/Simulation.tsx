import { Component, createMemo, createSignal, onCleanup, Show } from 'solid-js';
import { SimulationDocument } from '../generated/graphql';
import { Simulation } from '../wasm'
import EventLog from './EventLog';
import Graph from './Graph'
import QueryEditor from './QueryEditor';
import { throttle } from '@solid-primitives/scheduled';

const Sim: Component = () => {
    const simulation = new Simulation();
    onCleanup(() => {
        simulation.free();
    });

    const maxTick = 2000;

    let epoch = 0;
    const [epochWatcher, setEpochWatcher] = createSignal(epoch);

    const setTick = (tick: number) => {
        simulation.setTick(tick);
        epoch += 1;
        setEpochWatcher(epoch);
    };

    const step = () => {
        simulation.step();
        epoch += 1;
        setEpochWatcher(epoch);
    };

    // force refresh of this whenever epoch changes
    const simulationSignal = createMemo(() => {
        epochWatcher()
        return simulation;
    }, simulation, { equals: false });

    const simData = createMemo(() => simulationSignal().fetchUnchecked(SimulationDocument));
    const tick = () => simData().currentTick;

    const [playing, setPlaying] = createSignal(false);
    const timer = setInterval(() => {
        if (playing()) {
            setTick((tick() + 1) % maxTick);
        }
    }, 20);
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