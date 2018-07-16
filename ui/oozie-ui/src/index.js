import React from 'react';
import ReactDOM from 'react-dom';
import { Provider } from 'react-redux';
import './index.css';
import store from './store';
import OozieUI from './OozieUI';
import registerServiceWorker from './registerServiceWorker';

ReactDOM.render(
  <Provider store={store}>
    <OozieUI/>
  </Provider>,
  document.getElementById('root')
);
registerServiceWorker();
