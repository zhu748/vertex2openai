from abc import ABC, abstractmethod
from fastapi import Request
from models import OpenAIRequest

class BaseUpstream(ABC):
    """
    上游通道抽象基类
    统一暴露 chat_completions 接口，供路由层根据配置动态分发
    """
    @abstractmethod
    async def chat_completions(self, request_obj: OpenAIRequest, fastapi_request: Request):
        pass