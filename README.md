# **User Events Simulation**

This branch contains code for a basic simulation of user events -- that is, actions performed by users on podcast episodes. 

The code contained in `user_events.py` reads the data contained in `top_episodes.json` and simulates a situation where: 

- Users with a unique ID listen to podcast episodes. The number of users is arbitrarily set to 300 and the maximum number of episodes per user is arbitrarily set to 5
- On each episode they listen to, users perform one action among `pause`, `like`, `skip`, `rate`, `complete`
    -  Rating an episode is not compulsory: if users give no rating then `rating` is None, while if they do `rating` is a number between 1 and 5.
- Each action is timestamped

The code prints the following information to screen: 

- User ID
- Action performed
- The action's numeric ID
- Action timestamp
- Episode rating (if available)

That information is also saved to a `.json` file for subsequent analyses. 