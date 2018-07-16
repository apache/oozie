import { REQUEST, RECEIVE, FAILURE } from "../actions/oozie/workflows";

const data = (state) => state.workflowReducer;
export const getWorkflows = (state) => data(state).workflows || [];
export const getPagination = (state) => {
  if(data(state).workflows) {
    return {
      pageSize: data(state).len,
      total: data(state).total,
      current: data(state).offset
    }
  }

  return { pageSize: 0, current: 0, total: 0 };
};
export const isFetching = (state) => data(state).isFetching || false;
export const getMessage = (state) => data(state).message || null;

const workflowReducer = (state = {}, action) => {
  switch(action.type) {
    case REQUEST:
      return { ...state, isFetching: action.isFetching };
    case RECEIVE:
      return { ...state, workflows: action.workflows, total: action.total, offset: action.offset, len: action.len };
    case FAILURE:
      return { ...state, message: action.message };
    default:
      return state;
  }
};

export default workflowReducer;