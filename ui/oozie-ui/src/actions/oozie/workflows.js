import axios from 'axios';

export const WORKFLOWS_ENDPOINT = 'http://localhost:11000/oozie/v2/jobs';

export const REQUEST = '/actions/oozie/workflows/REQUEST';
export const RECEIVE = '/actions/oozie/workflows/RECEIVE';
export const FAILURE = '/actions/oozie/workflows/FAILURE';

export const requestWorkflows = (isFetching = true) => ({ type: REQUEST, isFetching: isFetching });
export const receiveWorkflows = (workflows, total, offset, len) => ({
  type: RECEIVE,
  workflows: workflows,
  total: total,
  offset: offset,
  len: len,
});
export const failureWorkflows = (message) => ({ type: FAILURE, message: message });

export const fetchWorkflows = (offset = 1, len = 20) => {
  let url = WORKFLOWS_ENDPOINT;

  return function(dispatch) {
    dispatch(requestWorkflows());

    return axios.get(url, { params: { offset: offset, len: len }})
      .then(response => {
        const { workflows, total, offset, len } = response.data;

        dispatch(receiveWorkflows(workflows, total, offset, len));
        dispatch(requestWorkflows(false));
      })
      .catch(error => {
        console.warn(error);
        dispatch(failureWorkflows(error));
        dispatch(requestWorkflows(false));
      });
  }
};
