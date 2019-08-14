package com.nytimes.android.sample

import android.os.Bundle
import android.view.View
import android.widget.Toast
import android.widget.Toast.makeText
import androidx.appcompat.app.AppCompatActivity
import androidx.appcompat.widget.Toolbar
import androidx.lifecycle.lifecycleScope
import androidx.recyclerview.widget.LinearLayoutManager
import com.nytimes.android.external.store3.pipeline.PipelineStore
import com.nytimes.android.external.store3.pipeline.StoreRequest
import com.nytimes.android.sample.data.model.Post
import com.nytimes.android.sample.reddit.PostAdapter
import kotlinx.android.synthetic.main.activity_store.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.isActive


class RoomPipelineActivity : AppCompatActivity() {
    lateinit var postAdapter: PostAdapter
    lateinit var persistedStore: PipelineStore<String, List<Post>>

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContentView(R.layout.activity_store)
        setSupportActionBar(findViewById<View>(R.id.toolbar) as Toolbar)

        postAdapter = PostAdapter()
        val layoutManager = LinearLayoutManager(this)
        layoutManager.orientation = LinearLayoutManager.VERTICAL
        postRecyclerView.layoutManager = layoutManager
        postRecyclerView.adapter = postAdapter
        persistedStore = (applicationContext as SampleApp).roomPipelineStore
        loadPosts()
    }

    fun loadPosts() {
        lifecycleScope.launchWhenStarted {
            try {
                persistedStore.stream(
                        StoreRequest.cached(
                                key = "aww",
                                refresh = true
                        )
                ).collect {
                    showPosts(it)
                }
            } catch (error: Throwable) {
                if (isActive) {
                    makeText(this@RoomPipelineActivity,
                            "Failed to load reddit posts: ${error.message}",
                            Toast.LENGTH_SHORT)
                            .show()
                }
            }
        }
    }

    private fun showPosts(posts: List<Post>) {
        postAdapter.submitList(posts)
        makeText(this@RoomPipelineActivity,
                "Loaded ${posts.size} posts",
                Toast.LENGTH_SHORT)
                .show()
    }
}
