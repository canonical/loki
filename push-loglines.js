import loki from 'k6/x/loki';

const KB = 1024;
const MB = KB * KB;

const conf = loki.Config("http://localhost:3100", 5000, 0.5, { "app": 1});
const client = loki.Client(conf);

export const options = {
   timeUnit: '1s',
   rate: 300,
   duration: '4h',
};

export default () => {
   client.pushParameterized(2, 500 * KB, 1 * MB);
};
