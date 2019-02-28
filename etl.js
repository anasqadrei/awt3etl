'use strict';

(async function() {
  require('dotenv').config()
  const MongoClient = require('mongodb').MongoClient

  // source
  const sourceUrl = process.env.SOURCE_DB_URL
  const sourceDbName = process.env.SOURCE_DB_NAME
  let sourceClient

  // target
  const targetUrl = process.env.TARGET_DB_URL
  const targetDbName = process.env.TARGET_DB_NAME
  let targetClient

  try {
    console.log("starting...")

    // connect to source
    sourceClient = await MongoClient.connect(sourceUrl, { useNewUrlParser: true })
    const sourceDb = sourceClient.db(sourceDbName)
    console.log("connected to source")

    // connect to target
    targetClient = await MongoClient.connect(targetUrl, { useNewUrlParser: true })
    const targetDb = targetClient.db(targetDbName)
    console.log("connected to target")

    // // copy and transform artists
    // try {
    //   await targetDb.collection('artists').drop()
    // } catch (e) {
    //   console.log(e);
    // }
    //
    // const artistCursor = await sourceDb.collection('artists').find({ $or: [{ _id: { $gte: 1, $lte: 50 } }, { _id: { $gte: 6701, $lte: 6705 } }]})
    // while(await artistCursor.hasNext()) {
    //   const doc = await artistCursor.next()
    //   doc._id = doc._id.toString()
    //   try {
    //     doc.commentsCount = await sourceDb.collection('comments').countDocuments({
    //       'reference.collection': 'artists',
    //       'reference.id': parseInt(doc._id, 10)
    //     })
    //   } catch (e) {
    //     console.log(e);
    //   }
    //   await targetDb.collection('artists').insertOne(doc)
    // }

    // // copy and transform blogposts
    // const blogpostsCursor = await sourceDb.collection('blogposts').find()
    // while(await blogpostsCursor.hasNext()) {
    //   const doc = await blogpostsCursor.next()
    //   doc._id = doc._id.toString()
    //   await targetDb.collection('blogposts').insertOne(doc)
    // }

    // // copy countries
    // const countryCursor = await sourceDb.collection('countries').find()
    // while(await countryCursor.hasNext()) {
    //   const doc = await countryCursor.next()
    //   await targetDb.collection('countries').insertOne(doc)
    // }

    // // copy and transform users
    // const userCursor = await sourceDb.collection('users').find({ $or: [{ _id: { $gte: 1, $lte: 15 } }, { _id: { $gte: 50455, $lte: 50505 } }]})
    // while(await userCursor.hasNext()) {
    //   const doc = await userCursor.next()
    //   doc._id = doc._id.toString()
    //   delete doc.recentlyPlayed
    //   await targetDb.collection('users').insertOne(doc)
    // }

    // // copy and transform comments
    // try {
    //   await targetDb.collection('comments').drop()
    // } catch (e) {
    //   console.log(e);
    // }
    // try {
    //   await targetDb.collection('usercomments').drop()
    // } catch (e) {
    //   console.log(e);
    // }
    //
    // const commentCursor = await sourceDb.collection('comments').find({ _id: { $gte: 1, $lte: 1500 } })
    // while(await commentCursor.hasNext()) {
    //   const doc = await commentCursor.next()
    //   doc._id = doc._id.toString()
    //   doc.reference.id = doc.reference.id.toString()
    //   if (doc.parent) {
    //     doc.parent = doc.parent.toString()
    //   }
    //   for (let i = 0; doc.children && i < doc.children.length; i++) {
    //     doc.children[i] = doc.children[i].toString()
    //   }
    //   doc.user = doc.user.toString()
    //   if (doc.likers) {
    //     doc.likeCount = doc.likers.length
    //     for (let i = 0; i < doc.likers.length; i++) {
    //       const liker = {
    //         _id: {
    //           user: doc.likers[i].toString(),
    //           comment: doc._id
    //         },
    //         like: true
    //       }
    //       await targetDb.collection('usercomments').insertOne(liker)
    //     }
    //   }
    //   delete doc.likers
    //   await targetDb.collection('comments').insertOne(doc)
    // }
    // TO DO: reference collection commentsCount to be inclusive of replies. done for artists

    // // copy and transform songs
    // console.log('songs')
    // const songCursor = await sourceDb.collection('songs').find({ $or: [{ _id: { $gte: 1, $lte: 5 } }, { _id: { $gte: 150000, $lte: 150005 } }]})
    // while(await songCursor.hasNext()) {
    //   const doc = await songCursor.next()
    //   doc._id = doc._id.toString()
    //   doc.artist = doc.artist.toString()
    //   doc.uploader = doc.uploader.toString()
    //   if (doc.images) {
    //     doc.imagesList = []
    //     for (let i = 0; i < doc.images.length; i++) {
    //       doc.imagesList[i] = doc.images[i].toString()
    //     }
    //     delete doc.images
    //   }
    //   if (doc.lyrics) {
    //     doc.lyrics = doc.lyrics.toString()
    //   }
    //   delete doc.videos
    //   delete doc.vidoesCount
    //   await targetDb.collection('songs').insertOne(doc)
    // }

    // // copy and transform song images
    // console.log('song images')
    // const songimageCursor = await sourceDb.collection('songimages').find({ $or: [{ _id: { $gte: 1, $lte: 200 } }]})
    // while(await songimageCursor.hasNext()) {
    //   const doc = await songimageCursor.next()
    //   doc._id = doc._id.toString()
    //   doc.song = doc.song.toString()
    //   doc.addedBy = doc.addedBy.toString()
    //   for (let i = 0; doc.likers && i < doc.likers.length; i++) {
    //     doc.likers[i] = doc.likers[i].toString()
    //   }
    //   for (let i = 0; doc.dislikers && i < doc.dislikers.length; i++) {
    //     doc.dislikers[i] = doc.dislikers[i].toString()
    //   }
    //   await targetDb.collection('songimages').insertOne(doc)
    // }

    // // copy and transform lyrics
    // console.log('lyrics')
    // const lyricsCursor = await sourceDb.collection('songlyrics').find({ $or: [{ _id: { $gte: 1, $lte: 10 } }]})
    // while(await lyricsCursor.hasNext()) {
    //   const doc = await lyricsCursor.next()
    //   doc._id = doc._id.toString()
    //   doc.song = doc.song.toString()
    //   doc.addedBy = doc.addedBy.toString()
    //   await targetDb.collection('songlyrics').insertOne(doc)
    // }

    console.log("done")
  } catch (err) {
    console.log(err.stack)
  }

  // disconnect
  if (sourceClient) {
    sourceClient.close()
  }
  if (targetClient) {
    targetClient.close()
  }
})()
