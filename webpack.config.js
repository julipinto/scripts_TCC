import path from 'path';

const config = {
  mode: 'none',
  target: 'node',
  entry: './src/index.js',
  output: {
    path: path.resolve('.', 'dist'),
    filename: 'queryrunner.js',
    libraryTarget: 'commonjs2', // adiciona essa linha
  },

  module: {
    rules: [
      {
        test: /\.js$/, // aplique a regra para arquivos JS
        exclude: /(node_modules|dist|out|queries)/, // exclua a pasta node_modules
        use: {
          loader: 'babel-loader',
          options: {
            presets: ['@babel/preset-env'],
          },
        },
      },
    ],
  },
};

export default config;
