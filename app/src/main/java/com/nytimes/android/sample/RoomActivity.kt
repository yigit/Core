package com.nytimes.android.sample

import android.os.Bundle
import android.view.View
import androidx.appcompat.app.AppCompatActivity
import androidx.appcompat.widget.Toolbar
import androidx.lifecycle.lifecycleScope
import com.google.android.material.snackbar.Snackbar
import com.nytimes.android.external.store3.pipeline.StoreRequest
import com.nytimes.android.external.store3.pipeline.StoreResponse
import com.nytimes.android.sample.reddit.PostAdapter
import kotlinx.android.synthetic.main.activity_room_store.pullToRefresh
import kotlinx.android.synthetic.main.activity_room_store.root
import kotlinx.android.synthetic.main.activity_store.postRecyclerView
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch

@FlowPreview
class RoomActivity : AppCompatActivity() {
    @ExperimentalCoroutinesApi
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_room_store)
        setSupportActionBar(findViewById<View>(R.id.toolbar) as Toolbar)
        initUI()

    }

    @ObsoleteCoroutinesApi
    @ExperimentalCoroutinesApi
    private fun initUI() {
        val adapter = PostAdapter()
        // lazily set the adapter when we have data the first time so that RecyclerView can
        // restore position
        fun setAdapterIfNotSet() {
            if (postRecyclerView.adapter == null) {
                postRecyclerView.adapter = adapter
            }
        }
        lifecycleScope.launchWhenStarted {
            val errors = Channel<String?>(Channel.CONFLATED)
            // use another channel to trigger refresh
            val refreshTrigger = Channel<Unit>(Channel.CONFLATED).also {
                it.send(Unit)
            }

            fun refresh() {
                launch {
                    // clear errors
                    errors.send(null)
                    refreshTrigger.send(Unit)
                }
            }

            pullToRefresh.setOnRefreshListener {
                refresh()
            }

            launch {
                errors.consumeAsFlow().collect {
                    it?.let {
                        Snackbar.make(root, it, Snackbar.LENGTH_INDEFINITE).setAction(
                            "refresh"
                        ) {
                            refresh()
                        }.show()
                    }
                }
            }
            refreshTrigger.consumeAsFlow().flatMapLatest {
                (application as SampleApp).roomPipeline.stream(
                    StoreRequest.cached(
                        key = "aww",
                        refresh = true
                    )
                )
            }.onEach {
                // if there is an error, send it to the error channel
                it.errorOrNull()?.let {
                    errors.send(it.localizedMessage)
                }
            }.collect {
                it.dataOrNull()?.let {
                    // don't set adapter unless adapter has data so that RV can recover scroll pos
                    setAdapterIfNotSet()
                }
                adapter.submitList(it.dataOrNull())
                pullToRefresh.isRefreshing = it is StoreResponse.Loading
            }
        }
    }
}
