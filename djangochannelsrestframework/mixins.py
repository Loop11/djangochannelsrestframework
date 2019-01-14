from rest_framework import status

from .decorators import action


class CreateModelMixin:
    @action()
    def create(self, data, **kwargs):

        serializer = self.get_serializer(data=data, action_kwargs=kwargs)
        serializer.is_valid(raise_exception=True)

        instance = self.perform_create(serializer, **kwargs)
        data = serializer.data

        if hasattr(instance, 'uuid'):
            data['__uuid'] = str(instance.uuid)
        if hasattr(instance, 'id'):
            data['__id'] = str(instance.uuid)
        data['__model'] = "%s.%s" % (instance._meta.app_label.lower(),
                                     instance._meta.object_name.lower())

        return data, status.HTTP_201_CREATED

    def perform_create(self, serializer, **kwargs):
        return serializer.save()


class ListModelMixin:
    @action()
    def list(self, **kwargs):
        queryset = self.filter_queryset(self.get_queryset(**kwargs), **kwargs)
        serializer = self.get_serializer(
            instance=queryset, many=True, action_kwargs=kwargs)
        return serializer.data, status.HTTP_200_OK


class RetrieveModelMixin:
    @action()
    def retrieve(self, **kwargs):
        instance = self.get_object(**kwargs)
        serializer = self.get_serializer(
            instance=instance, action_kwargs=kwargs)

        data = serializer.data

        if hasattr(instance, 'uuid'):
            data['__uuid'] = str(instance.uuid)
        data['__pk'] = str(instance.pk)
        data['__model'] = "%s.%s" % (instance._meta.app_label.lower(),
                                     instance._meta.object_name.lower())

        return serializer.data, status.HTTP_200_OK


class UpdateModelMixin:
    @action()
    def update(self, data, **kwargs):
        instance = self.get_object(data=data, **kwargs)

        serializer = self.get_serializer(
            instance=instance, data=data, action_kwargs=kwargs, partial=False)

        serializer.is_valid(raise_exception=True)
        instance = self.perform_update(serializer, **kwargs)
        data = serializer.data

        if getattr(instance, '_prefetched_objects_cache', None):
            # If 'prefetch_related' has been applied to a queryset, we need to
            # forcibly invalidate the prefetch cache on the instance.
            instance._prefetched_objects_cache = {}

        if hasattr(instance, 'uuid'):
            data['__uuid'] = str(instance.uuid)
        data['__pk'] = str(instance.pk)
        data['__model'] = "%s.%s" % (instance._meta.app_label.lower(),
                                     instance._meta.object_name.lower())

        return data, status.HTTP_200_OK

    def perform_update(self, serializer, **kwargs):
        return serializer.save()


class PatchModelMixin:
    @action()
    def patch(self, data, **kwargs):
        instance = self.get_object(data=data, **kwargs)

        serializer = self.get_serializer(
            instance=instance, data=data, action_kwargs=kwargs, partial=True)

        serializer.is_valid(raise_exception=True)
        self.perform_patch(serializer, **kwargs)

        if getattr(instance, '_prefetched_objects_cache', None):
            # If 'prefetch_related' has been applied to a queryset, we need to
            # forcibly invalidate the prefetch cache on the instance.
            instance._prefetched_objects_cache = {}

        return serializer.data, status.HTTP_200_OK

    def perform_patch(self, serializer, **kwargs):
        serializer.save()


class DeleteModelMixin:
    @action()
    def delete(self, **kwargs):

        instance = self.get_object(**kwargs)

        serializer = self.get_serializer(
            instance=instance, data=data, action_kwargs=kwargs, partial=False)

        # Return some data of what was
        data = serializer.data
        if hasattr(instance, 'uuid'):
            data['__uuid'] = str(instance.uuid)
        data['__pk'] = str(instance.pk)
        data['__model'] = "%s.%s" % (instance._meta.app_label.lower(),
                                     instance._meta.object_name.lower())

        instance.delete()

        return data, status.HTTP_200_OK

    def perform_delete(self, instance, **kwargs):
        instance.delete()
