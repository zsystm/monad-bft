/* @refresh reload */
import { render } from 'solid-js/web';

import './index.css';
import { Route, Router } from '@solidjs/router';
import Live from './routes/Live';
import NotFound from './routes/NotFound';
import Block from './routes/Block';

const root = document.getElementById('root');

if (import.meta.env.DEV && !(root instanceof HTMLElement)) {
  throw new Error(
    'Root element not found. Did you forget to add it to your index.html? Or maybe the id attribute got misspelled?',
  );
}

render(
  () => (
    <Router>
      <Route path='/' component={Live}></Route>
      <Route path='/block/:id' component={Block}></Route>
      <Route path="*404" component={NotFound} />
    </Router>
  ),
  root!
);
