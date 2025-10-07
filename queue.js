const express = require('express')

// bull v3

// bullmq v1+
const { Queue: BullMQQueue } = require('bullmq')

const { createBullBoard } = require('bull-board')
const { BullAdapter } = require('bull-board/bullAdapter')
const { BullMQAdapter } = require('bull-board/bullMQAdapter')
const Bull = require('bull')

// instantiate your Bull (v3) queues
const someQueue      = new Bull('campaign-bulk',      { redis: { host: 'localhost', port: 6379 } })
const someOtherQueue = new Bull('campaign-bulk-1', { redis: { host: 'localhost', port: 6379 } })

// instantiate your BullMQ (v1) queue
const queueMQ = new BullMQQueue('queueMQName', {
  connection: { host: 'localhost', port: 6379 }
})

const { router, setQueues } = createBullBoard([
  new BullAdapter(someQueue),
  new BullAdapter(someOtherQueue),
  new BullMQAdapter(queueMQ),
])

const app = express()
app.use('/admin/queues', router)

app.listen(3000, () => {
  console.log('🛠️  bull-board listening on http://localhost:3000/admin/queues')
})
