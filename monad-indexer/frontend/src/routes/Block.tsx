import { useParams } from '@solidjs/router';
import { Component } from 'solid-js';

const Block: Component = () => {
    const params = useParams();

    return (
        <div>
            {params.id}
        </div>
    );
};

export default Block;
