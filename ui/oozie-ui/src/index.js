import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import OozieUI from './OozieUI';
import registerServiceWorker from './registerServiceWorker';

ReactDOM.render(<OozieUI/>, document.getElementById('root'));
registerServiceWorker();
