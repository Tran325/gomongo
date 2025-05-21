# Overview

This AI-powered stock trading bot leverages the TimeMixer model—a hybrid LSTM-attention architecture—to forecast price movements with high accuracy. By analyzing historical OHLCV data and market trends, the bot generates low-latency trading signals for intraday or swing strategies. The TimeMixer's ability to capture long-term dependencies and key temporal patterns makes it ideal for volatile equity markets. Integrated with broker APIs, the system executes trades autonomously while managing risk through dynamic stop-loss and position sizing.

## Model Output

Trained on 2019-2021 stock data, tested on 2022 with a profit of $480.45:

![Google Stock Trading episode](./extra/1.png)

You can obtain similar visualizations of your model evaluations using the [notebook](./visualize.ipynb) provided.

## Table of contents
  * [Models](#models)
  * [Ranking](#Ranking-in-2024)
  * [Realtime Agent](realtime-agent)
  * [Data Explorations](#data-explorations)
  * [Simulations](#simulations)
  * [Tensorflow-js](#tensorflow-js)
  * [Misc](#misc)
  * [Results](#results)
    * [Results Agent](#results-agent)
    * [Results signal prediction](#results-signal-prediction)
    * [Results analysis](#results-analysis)
    * [Results simulation](#results-simulation)

## contents

### Models

 1. LSTM
 2. LSTM Bidirectional
 3. LSTM 2-Path
 4. GRU
 5. GRU Bidirectional
 6. GRU 2-Path
 7. Vanilla
 8. Vanilla Bidirectional
 9. Vanilla 2-Path
 10. LSTM Seq2seq
 11. LSTM Bidirectional Seq2seq
 12. LSTM Seq2seq VAE
 13. GRU Seq2seq
 14. GRU Bidirectional Seq2seq
 15. GRU Seq2seq VAE
 16. Attention-is-all-you-Need
 17. CNN-Seq2seq
 18. Dilated-CNN-Seq2seq

    You can check the **Deep-learning models** [here](deep-learning)

### Ranking in 2024
1. TimeGPT ranking 1 (paid)
2. TimeFM ranking 2 (open source)
3. Chronos ranking 3 (open source)

    You can check the model Ranking in [here](https://arxiv.org/abs/2410.16032)

## Introduction

Generally, Reinforcement Learning is a family of machine learning techniques that allow us to create intelligent agents that learn from the environment by interacting with it, as they learn an optimal policy by trial and error. This is especially useful in many real world tasks where supervised learning might not be the best approach due to various reasons like nature of task itself, lack of appropriate labelled data, etc.

The important idea here is that this technique can be applied to any real world task that can be described loosely as a Markovian process.

## Approach

This work uses a Model-free Reinforcement Learning technique called Deep Q-Learning (neural variant of Q-Learning).
At any given time (episode), an agent abserves it's current state (n-day window stock price representation), selects and performs an action (buy/sell/hold), observes a subsequent state, receives some reward signal (difference in portfolio position) and lastly adjusts it's parameters based on the gradient of the loss computed.

There have been several improvements to the Q-learning algorithm over the years, and a few have been implemented in this project:

- [x] Vanilla DQN
- [x] DQN with fixed target distribution
- [x] Double DQN
- [ ] Prioritized Experience Replay
- [ ] Dueling Network Architectures


## Some Caveats

- At any given state, the agent can only decide to buy/sell one stock at a time. This is done to keep things as simple as possible as the problem of deciding how much stock to buy/sell is one of portfolio redistribution.
- The n-day window feature representation is a vector of subsequent differences in Adjusted Closing price of the stock we're trading followed by a sigmoid operation, done in order to normalize the values to the range [0, 1].
- Training is prefferably done on CPU due to it's sequential manner, after each episode of trading we replay the experience (1 epoch over a small minibatch) and update model parameters.

## Data

You can download Historical Financial data from [Yahoo! Finance](https://ca.finance.yahoo.com/) for training, or even use some sample datasets already present under `data/`.

## Getting Started

In order to use this project, you'll need to install the required python packages:

```bash
pip3 install -r requirements.txt
```

Now you can open up a terminal and start training the agent:

```bash
python3 train.py data/GOOG.csv data/GOOG_2018.csv --strategy t-dqn
```

Once you're done training, run the evaluation script and let the agent make trading decisions:

```bash
python3 eval.py data/GOOG_2019.csv --model-name model_GOOG_50 --debug
```

Now you are all set up!

## Acknowledgements

- [@keon](https://github.com/keon) for [deep-q-learning](https://github.com/keon/deep-q-learning)
- [@edwardhdlu](https://github.com/edwardhdlu) for [q-trader](https://github.com/edwardhdlu/q-trader)

## References

- [Playing Atari with Deep Reinforcement Learning](https://arxiv.org/abs/1312.5602)
- [Human Level Control Through Deep Reinforcement Learning](https://deepmind.com/research/publications/human-level-control-through-deep-reinforcement-learning/)
- [Deep Reinforcement Learning with Double Q-Learning](https://arxiv.org/abs/1509.06461)
- [Prioritized Experience Replay](https://arxiv.org/abs/1511.05952)
- [Dueling Network Architectures for Deep Reinforcement Learning](https://arxiv.org/abs/1511.06581)