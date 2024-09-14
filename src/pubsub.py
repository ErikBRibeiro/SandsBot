"""Provides classes for implementing Publisher / Subscriber relationships.

This module allows users to implement publisher / subscriber patterns in their
code by implementing an abstract class Subscriber and by utilizing the concrete
class Publisher to notify all attached subscribers of any update / notification.

Typical usage example:
  subscriber_a: Subscriber
  subscriber_b: Subscriber

  events = Publisher()
  events.subscribe(subscriber_a)
  events.subscribe(subscriber_b)

  events.notify("hello", "how are you")
  # subscribers A and B are called with update("hello", "how are you")
"""
from abc import ABC, abstractmethod
from typing import Optional

class Subscriber(ABC): # pylint: disable=too-few-public-methods
    """Acts upon updates from a Publisher

    This class is used as an interface for Publishers to contain a list of
    classes able to be updated with event notifications, because of this reason
    all classes who wish to utilize the pubsub mechanism must inherit from
    Subscriber and implement its update method, which is called every time a
    Publisher notifies an attached Subscriber about a new event.
    """
    @abstractmethod
    def update(self, identifier: str,
               event: str, data: object) -> None:
        """Act upon a new event published by a subscribed Publisher.

        This method is called everytime a publisher which this subscriber is
        attached to emits a new event, it's expected but not enforced for this
        method to be overriden and match on the event string, using it to
        typecheck the data associated as well as react to different events.

        Args:
          identifier: A string identifying the publisher of the event.
          event: A string describing the event.
          data:
            An object with extra data about the associated event. May be None.
        """

class Publisher:
    """Publishes events to all subscribers interested in the subject.

    Subscribers can attach themselves to a publisher by calling the publisher's
    subscribe method, where they will then start being updated on every call of
    the publisher's notify method with an event and its accompanying data.

    Subscribers who wish to stop receiving updates from a publisher may detach
    themselves by calling the publisher's unsubscribe method.

    Attributes:
      identifier:
        A string identifying the publisher, useful for when you want a
        subscriber to be able to handle many instances of the same
        publisher, or separate publishers of different classes.
        It is recommended for code utilising the Publisher class to identify
        itself as its class and specific instance, but this is not enforced.
      subscribers: A list of all Subscribers currently listening for events.
    """
    _subscribers: list[Subscriber]
    _identifier: str

    def __init__(self, identifier: str,
                 subscribers: Optional[list[Subscriber]] = None):
        self._identifier = identifier
        self._subscribers = subscribers if subscribers else []

    def subscribe(self, subscriber: Subscriber) -> None:
        """Attaches a subscriber to the Publisher.

        Adds subscriber to publisher's subscriber list, making it get notified
        on every update given by the publisher.

        Args:
          subscriber:
            An instace of a class that implements the Subscriber abstract
            class.
        """
        self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber: Subscriber) -> None:
        """Detaches a subscriber from the Publisher.

        Removes the subscriber from the publisher's subscriber list, stopping
        it from getting notified on subsequent updates made by the publisher.

        Args:
          subscriber:
            An instace of a class that implements the Subscriber abstract
            class.
        """
        for idx, sub in enumerate(self.subscribers):
            if sub == subscriber:
                self._subscribers.pop(idx)

    def notify(self, event: str, data: object = None) -> None:
        """Notifies all subscribers about an event, with accompanying data.

        Calls all subscribers in the publisher's subscription list with their
        update method, passing in the publisher's identifier, an event for the
        subscriber to match on, and associated data, which may be any object.

        Args:
          event:
            A string identifying the event that just occurred.
            It is expected but not enforced that this event identifier is
            unique enough for subscribers to match on it for typing information
            as well as for skipping undesired notifications.
          data:
            An object containing data about the event, which may be None.
        """
        for subscriber in self.subscribers:
            subscriber.update(self._identifier, event, data)

    @property
    def subscribers(self) -> list[Subscriber]:
        """Subscribers to be updated on the next notification.

        Returns:
          A list of subscribers associated with this publisher who will be
          called the next time someone utilizes the notify method of the
          publisher. This list can be modified at any time with the subscribe
          and unsubscribe methods.
        """
        return self._subscribers

    @property
    def identifier(self) -> str:
        """Identifies the publisher to subscribers when notifying events.

        This property is used for publishers to match on specific classes or
        instances of a publisher, such that it is possible to track more than
        one instance of publisher without relying on unique events.

        It is only forced that this property is set, but there isn't any
        validation done that it is unique or even that it is non-empty, such
        validations should be done in client code.

        Returns:
          A string identifying the publisher, may be empty or non-unique.
        """
        return self._identifier
