import { combineReducers } from 'redux';
import workflowReducer from './workflow/reducer';

const rootReducer = combineReducers({
  workflowReducer,
});

export default rootReducer;