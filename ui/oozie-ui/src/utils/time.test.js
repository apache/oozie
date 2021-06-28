import { readableDuration } from './time';

describe('readableDuration test suite', () => {
  test('should not convert incorrect values', () => {
    expect(readableDuration(-10000)).toEqual('N/a');
    expect(readableDuration(0)).toEqual('N/a');
  });

  test('should round', () => {
    expect(readableDuration(400)).toEqual('0s');
    expect(readableDuration(501)).toEqual('1s');
  });

  test('should convert seconds', () => {
    expect(readableDuration(10000)).toEqual('10s');
  });

  test('should convert minutes', () => {
    expect(readableDuration(71500)).toEqual('1m12s');
  });

  test('should convert hours', () => {
    expect(readableDuration((3600 * 17 + 5 * 60 + 59) * 1000)).toEqual('17h5m59s');
  });

  test('should convert days', () => {
    expect(readableDuration((3600 * 24 * 3 + 3600 * 9 + 60 + 3) * 1000)).toEqual('3d9h1m3s');
  });

  test('should convert skip zero unit', () => {
    expect(readableDuration((3600 * 24) * 1000)).toEqual('1d');
    expect(readableDuration((3600 * 24 + 120) * 1000)).toEqual('1d2m');
    expect(readableDuration((3600 * 24 + 1) * 1000)).toEqual('1d1s');
    expect(readableDuration((3600 * 24 + 3600) * 1000)).toEqual('1d1h');
  });
});
