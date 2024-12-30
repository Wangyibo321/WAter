from openai import OpenAI, APIError
import openai
import json, os
import random
import math


def shuffle_dict(dic):
    items = list(dic.items())
    random.shuffle(items)
    dic = dict(items)
    return dic

def split_dict(ori_dict, batch_size):
    keys = list(ori_dict.keys())
    batchs = [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]
    subdicts = [{key: ori_dict[key] for key in batch} for batch in batchs]
    
    return subdicts
