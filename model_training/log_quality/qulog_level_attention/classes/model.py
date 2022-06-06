import copy
import torch.nn as nn
from networks import MultiHeadedAttention, PositionalEncoding, PositionwiseFeedForward
from networks import Encoder, EncoderDecoder, EncoderLayer
from networks import Decoder, DecoderLayer
from networks import Generator, Embeddings


class NuLogsyModel:
    def __init__(self, src_vocab, tgt_vocab, n_layers=3,
                 in_features=512, out_features=2048, num_heads=8, dropout=0.1, max_len=20):
        """Construct a model from hyper parameters.
        Parameters
        ----------
        src_vocab : int
            Length of source vocabulary.
        tgt_vocab : int
            Length of target vocabulary
        n_layers : int
            Number of encoder and decoder layers.
        in_features : int
            number of input features
        out_features : int
            number of output features
        dropout : float
            Dropout weights percentage
        max_len : int
        num_heads : int
            Number of heads for the multi-head model


        """
        c = copy.deepcopy
        attn = MultiHeadedAttention(num_heads, in_features)
        ff = PositionwiseFeedForward(in_features, out_features, dropout)
        position = PositionalEncoding(in_features, dropout, max_len)
        self.model = EncoderDecoder(
            Encoder(EncoderLayer(in_features, c(attn), c(ff), dropout), n_layers),
            Decoder(DecoderLayer(in_features, c(attn), c(attn), c(ff), dropout), n_layers),
            nn.Sequential(Embeddings(in_features, src_vocab), c(position)),
            nn.Sequential(Embeddings(in_features, tgt_vocab), c(position)),
            Generator(in_features, tgt_vocab))
        # This was important from their code.
        # Initialize parameters with Glorot / fan_avg.
        for p in self.model.parameters():
            if p.dim() > 1:
                nn.init.xavier_uniform_(p)

    def get_model(self):
        return self.model

