import axios from 'axios';

export const WORKFLOWS_ENDPOINT = 'http://localhost:12000/oozie/v2/jobs';

export const REQUEST = '/actions/oozie/workflows/REQUEST';
export const RECEIVE = '/actions/oozie/workflows/RECEIVE';
export const FAILURE = '/actions/oozie/workflows/FAILURE';

export const STATUS = {
  PREP: 'PREP',
  RUNNING: 'RUNNING',
  SUCCEEDED: 'SUCCEEDED',
  KILLED: 'KILLED',
  FAILED: 'FAILED',
  SUSPENDED: 'SUSPENDED',
};

export const COMPLETED_STATUSES = [STATUS.KILLED, STATUS.SUCCEEDED, STATUS.FAILED];
export const RUNNING_STATUSES = [STATUS.PREP, STATUS.RUNNING, STATUS.SUSPENDED];

export const requestWorkflows = (isFetching = true, params = {}) => ({
  type: REQUEST,
  isFetching: isFetching,
  params: params
});

export const receiveWorkflows = (workflows, params) => ({
  type: RECEIVE,
  workflows: workflows,
  params: params
});

export const failureWorkflows = (message, params) => ({
  type: FAILURE,
  message: message,
  params: params
});

export const fetchCompletedWorkflows = (offset, len) => fetchWorkflows(COMPLETED_STATUSES, offset, len);
export const fetchRunningWorkflows = (offset, len) => fetchWorkflows(RUNNING_STATUSES, offset, len);

const fetchWorkflows = (statuses, offset = 1, len = 20) => {
  const url = WORKFLOWS_ENDPOINT;

  return function(dispatch) {
    dispatch(requestWorkflows(true, { statuses, offset, len }));

    const filter = statuses.map(s => `status=${s}`).join(';');

    return axios.get(url, { params: { filter, offset, len }})
      .then(response => {
        const { workflows, total, offset, len } = response.data;

        dispatch(receiveWorkflows(workflows, { statuses, total, offset, len }));
      })
      .catch(error => {
        console.warn(error);
        dispatch(failureWorkflows(error, { statuses, offset, len }));
      })
      .then(() => dispatch(requestWorkflows(false, { statuses, offset, len })));
  }
};

export const isEqualStatuses = (a, b) => {
  return a.length === b.length && a.every((e,i) => e === b[i]);
};
