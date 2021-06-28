import {
  REQUEST, RECEIVE, FAILURE, COMPLETED_STATUSES, isEqualStatuses
} from "../actions/oozie/workflows";

const completed = (state) => state.workflowReducer.completed || {};
const running = (state) => state.workflowReducer.running || {};

export const getCompletedWorkflows = (state) => completed(state).workflows || [];
export const getRunningWorkflows = (state) => running(state).workflows || [];

export const getCompletedPagination = (state) => getPagination(state, true);
export const getRunningPagination = (state) => getPagination(state, false);

const getPagination = (state, getCompletedStatus = false) => {
  const s = getCompletedStatus ? completed(state) : running(state);

  return s.workflows ?
    { pageSize: s.len, total: s.total, current: s.offset } :
    { pageSize: 0, total: 0, current: 0 };
};

export const isCompletedFetching = (state) => completed(state).isFetching || false;
export const isRunningFetching = (state) => running(state).isFetching || false;

export const getCompletedMessage = (state) => completed(state).message || null;
export const getRunningMessage = (state) => running(state).message || null;

const dataByStatus = (action, state, data) => {
  return isEqualStatuses(COMPLETED_STATUSES, action.params.statuses) ?
    {...state, completed: {...state.completed, ...data}} :
    {...state, running: {...state.running, ...data}};
};

const workflowReducer = (state = {}, action) => {
  switch(action.type) {
    case REQUEST:
      return dataByStatus(action, state, {
        isFetching: action.isFetching
      });
    case RECEIVE:
      return dataByStatus(action, state, {
        workflows: action.workflows,
        total: action.params.total,
        offset: action.params.offset,
        len: action.params.len
      });
    case FAILURE:
      return dataByStatus(action, state, {
        message: action.message
      });
    default:
      return state;
  }
};

export default workflowReducer;