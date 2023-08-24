import ora from 'ora';

export default class SpinnerLog {
  spinner = null;
  database = null;

  constructor(database) {
    this.spinner = ora('logs');
    this.database = database;
  }

  connectDB() {
    this.spinner.start(`Connecting to ${this.database} database`).start();
  }

  connectedDB() {
    this.spinner.succeed(`Connected to ${this.database} database`);
  }

  errorConnectDB(error) {
    this.spinner.fail(`Error connecting to ${this.database} database`);
    console.error(error);
  }
}
