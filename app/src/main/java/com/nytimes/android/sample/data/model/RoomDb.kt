package com.nytimes.android.sample.data.model

import androidx.room.*
import com.squareup.moshi.Moshi
import kotlinx.coroutines.flow.Flow

@Entity(
        primaryKeys = ["url"]
)
data class PostEntity(
        val position: Int,
        val subreddit: String,
        @Embedded
        val post : Post
)

private class PreviewAdapter {
    private val moshi = Moshi.Builder().build()
    private val adapter = moshi.adapter<Preview>(Preview::class.java)
    @TypeConverter
    fun toPreview(data : String?) : Preview? {
        if (data == null) {
            return null
        }
        return adapter.fromJson(data)
    }
    @TypeConverter
    fun fromPreview(preview: Preview?) : String? {
        if (preview == null) {
            return null
        }
        return adapter.toJson(preview)
    }
}

@Dao
abstract class PostDao {
    @Transaction
    open suspend fun savePosts(subreddit: String, posts : List<Post>) {
        // first delete the previous data and then insert the new ones
        deletePosts(subreddit)
        val entities = posts.mapIndexed { index: Int, post: Post ->
            PostEntity(
                    position = index,
                    post = post,
                    subreddit = subreddit
            )
        }
        insertPosts(entities)
    }

    @Query("DELETE FROM PostEntity WHERE subreddit = :subreddit")
    protected abstract suspend fun deletePosts(subreddit: String)

    @Insert
    protected abstract suspend fun insertPosts(posts : List<PostEntity>)

    @Query("SELECT * FROM PostEntity WHERE subreddit = :subreddit ORDER BY position ASC")
    abstract fun getPosts(subreddit: String) : Flow<List<Post>>
}

@Database(
        entities = [PostEntity::class],
        version = 1,
        exportSchema = false)
@TypeConverters(
        PreviewAdapter::class
)
abstract class RedditDb : RoomDatabase() {
    abstract fun dao() : PostDao
}