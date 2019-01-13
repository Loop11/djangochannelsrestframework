from abc import abstractmethod
from functools import partial
from typing import Dict, Type

from channels.db import database_sync_to_async
from django.db.models import Model
from djangochannelsrestframework.consumers import APIConsumerMetaclass
from djangochannelsrestframework.decorators import action
from djangochannelsrestframework.generics import GenericAsyncAPIConsumer
from djangochannelsrestframework.mixins import RetrieveModelMixin
from djangochannelsrestframework.observer import ModelObserver
from rest_framework import status


class _GenericModelObserver:
    def __init__(self, func, **kwargs):
        self.func = func
        self._group_names = None
        self._serializer = None

    def bind_to_model(self,
                      model_cls: Type[Model],
                      group_name_prefix=None,
                      group_names_func=None,
                      serializer_class=None,
                      stream=None) -> ModelObserver:
        observer = ModelObserver(
            func=self.func,
            model_cls=model_cls,
            group_name_prefix=group_name_prefix,
            group_names_func=group_names_func,
            serializer_class=serializer_class,
            stream=stream)

        observer.groups(self._group_names)
        observer.serializer(self._serializer)
        return observer

    def groups(self, func):
        self._group_names = func
        return self

    def serializer(self, func):
        self._serializer = func
        return self


class ObserverAPIConsumerMetaclass(APIConsumerMetaclass):
    def __new__(mcs, name, bases, namespace) -> Type[GenericAsyncAPIConsumer]:

        queryset = namespace.get('queryset', None)
        if queryset is not None:

            group_name_prefix = namespace.get('group_name_prefix', '')
            group_names_func = namespace.get('get_group_names', None)
            serializer_class = namespace.get('serializer_class', None)
            stream = namespace.get('stream', None)

            for attr_name, attr in namespace.items():
                if isinstance(attr, _GenericModelObserver):
                    namespace[attr_name] = attr.bind_to_model(
                        model_cls=queryset.model,
                        group_name_prefix=group_name_prefix,
                        group_names_func=group_names_func,
                        serializer_class=serializer_class,
                        stream=stream)

            for base in bases:
                group_name_prefix = getattr(base, 'group_name_prefix',
                                            group_name_prefix)
                group_names_func = getattr(base, 'get_group_names',
                                           group_names_func)
                stream = getattr(base, 'stream', stream)

                new_serializer_class = getattr(base, 'serializer_class',
                                               serializer_class)
                if new_serializer_class is not None:
                    serializer_class = new_serializer_class

                for attr_name in dir(base):
                    attr = getattr(base, attr_name)
                    if isinstance(attr, _GenericModelObserver):
                        namespace[attr_name] = attr.bind_to_model(
                            model_cls=queryset.model,
                            group_name_prefix=group_name_prefix,
                            group_names_func=group_names_func,
                            serializer_class=serializer_class,
                            stream=stream)

        return super().__new__(mcs, name, bases, namespace)


class ObserverConsumerMixin(metaclass=ObserverAPIConsumerMetaclass):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subscribed_requests = {}  # type: Dict[function, str]


class ObserverModelInstanceMixin(ObserverConsumerMixin, RetrieveModelMixin):
    @abstractmethod
    def group_names(self, instance):
        raise NotImplementedError()

    @action()
    async def subscribe_instance(self, request_id=None, **kwargs):
        if request_id is None:
            raise ValueError('request_id must have a value set')
        # subscribe!
        instance = await database_sync_to_async(self.get_object)(**kwargs)
        await self.handle_instance_change.subscribe(instance=instance)
        self.subscribed_requests[self.__class__.
                                 handle_instance_change] = request_id

        return None, status.HTTP_201_CREATED

    @action()
    async def unsubscribe_instance(self, request_id=None, **kwargs):
        if request_id is None:
            raise ValueError('request_id must have a value set')
        # subscribe!
        instance = await database_sync_to_async(self.get_object)(**kwargs)
        await self.handle_instance_change.unsubscribe(instance=instance)
        del self.subscribed_requests[self.__class__.handle_instance_change]

        return None, status.HTTP_204_NO_CONTENT

    @_GenericModelObserver
    async def handle_instance_change(self, message, **kwargs):
        action = message.pop('action')
        message.pop('type')

        await self.handle_observed_action(
            action=action,
            request_id=self.subscribed_requests.get(
                self.__class__.handle_instance_change),
            **message)

    @handle_instance_change.groups
    def handle_instance_change(self: ModelObserver, instance, *args, **kwargs):

        if hasattr(self, 'group_names_func'):
            for group_name in self.group_names_func(self, instance):
                yield group_name
            return
        yield f"{self.group_name_prefix}-{instance.uuid}"

    async def handle_observed_action(self, action: str, request_id: str,
                                     **kwargs):
        """
        run the action.
        """
        try:
            await self.check_permissions(action, **kwargs)

            reply = partial(self.reply, action=action, request_id=request_id)

            if action == 'delete':
                await reply(data=kwargs, status=204)
                # send the delete
                return

            # the @action decorator will wrap non-async action into async ones.

            response = await self.retrieve(
                request_id=request_id, action=action, **kwargs)

            if isinstance(response, tuple):
                data, status = response
                await reply(data=data, status=status)

        except Exception as exc:
            await self.handle_exception(
                exc, action=action, request_id=request_id)
