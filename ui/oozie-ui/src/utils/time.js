export const readableDuration = (durationMills) => {
  if (durationMills <= 0) {
    return "N/a";
  } else {
    const s = Math.round(durationMills / 1000);
    const days = Math.floor(s / (24 * 3600));
    const hours = Math.floor(s % (24 * 3600) / 3600);
    const minutes = Math.floor(s % 3600 / 60);
    const seconds = s % 60;

    if (s === 0) {
      return '0s'; // less than a 1/2 second just round to 0
    } else if (s < 60) {
      return readableSecond(seconds);
    } else if (s < 3600) {
      return readableMinute(minutes, seconds);
    } else if (s < (24 * 3600)) {
      return readableHour(hours, minutes, seconds);
    } else {
      return readableDay(days, hours, minutes, seconds);
    }
  }
};

const readableDay = (days, hours, minutes, seconds) => {
  return days === 0 ?
    `${readableHour(hours, minutes, seconds)}` :
    `${days}d${readableHour(hours, minutes, seconds)}`;
};

const readableHour = (hours, minutes, seconds) => {
  return hours === 0 ?
    `${readableMinute(minutes, seconds)}` :
    `${hours}h${readableMinute(minutes, seconds)}`;
};

const readableMinute = (minutes, seconds) => {
  return minutes === 0 ?
    `${readableSecond(seconds)}` :
    `${minutes}m${readableSecond(seconds)}`;
};

const readableSecond = (seconds) => {
  return seconds === 0 ? '' : `${seconds}s`;
};

