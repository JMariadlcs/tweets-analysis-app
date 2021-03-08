package cdistRest;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;

public class Tweet {

    public static Tweet deserializeJson(String input_json) {
        Gson gson = new Gson();
        Tweet new_Tweet = gson.fromJson(input_json, Tweet.class);
        return new_Tweet;
  }

  public static List<Tweet> deserializeJsonArray(String input_json) {
        Gson gson = new Gson();
        Type collectionType = new TypeToken<Collection<Tweet>>() {}.getType();
        Collection<Tweet> new_tweet_collection = gson.fromJson(input_json,
                      collectionType);
        ArrayList<Tweet> new_Tweet_list = new ArrayList<Tweet>(
                      new_tweet_collection);

        return new_Tweet_list;
  }

  @SerializedName("geo")
  public String geo;

  @SerializedName("in_reply_to_status_id")
  public String in_reply_to_status_id;

  @SerializedName("truncated")
  public String truncated;

  @SerializedName("created_at")
  public String created_at;

  @SerializedName("retweet_count")
  public String retweet_count;

  @SerializedName("in_reply_to_user_id")
  public String in_reply_to_user_id;

  @SerializedName("id_str")
  public String id_str;

  @SerializedName("place")
  public transient String place;

  @SerializedName("favorited")
  public boolean favorited;

  @SerializedName("source")
  public String source;

  @SerializedName("in_reply_to_screen_name")
  public String in_reply_to_screen_name;

  @SerializedName("in_reply_to_status_id_str")
  public String in_reply_to_status_id_str;

  @SerializedName("id")
  public long id;

  @SerializedName("contributors")
  public String contributors;

  @SerializedName("coordinates")
  public String coordinates;

  @SerializedName("retweeted")
  public boolean retweeted;

  @SerializedName("text")
  public String text;

  @SerializedName("profile_image_url")
  public String profile_image_url;

  public User user;

}
