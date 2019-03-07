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
    console.time('all')
    console.log("starting...")

    // connect to source
    sourceClient = await MongoClient.connect(sourceUrl, { useNewUrlParser: true })
    const sourceDb = sourceClient.db(sourceDbName)
    console.log("connected to source")

    // connect to target
    targetClient = await MongoClient.connect(targetUrl, { useNewUrlParser: true })
    const targetDb = targetClient.db(targetDbName)
    console.log("connected to target")

    // copy and transform artists
    async function artistsETL() {
      // start
      const TARGET_COLLECTION = 'artists'
      console.time(TARGET_COLLECTION)
      console.log(`start ${TARGET_COLLECTION}`)
      try {
        await targetDb.collection(TARGET_COLLECTION).drop()
      } catch (e) {
        console.log(e);
      }

      // indexes
      await targetDb.collection(TARGET_COLLECTION).createIndex( { "name": 1 }, { unique: true, background: true } )

      // etl
      const artistCursor = await sourceDb.collection('artists').find({ $or: [{ _id: { $gte: 1, $lte: 5 } }, { _id: { $gte: 6701, $lte: 6705 } }]})
      while(await artistCursor.hasNext()) {
        const doc = await artistCursor.next()
        const newDoc = {
          _id: doc._id.toString(),
          name: doc.name,
          createdDate: doc.createdDate,
          image: doc.image,
          likes: doc.likersCount,
          songs: doc.songsCount,
          songPlays: doc.songsPlaysCount,
          songListeners: doc.songsListenersCount,
          songDownloads: doc.songsDownloadsCount,
          songLikes: doc.songsLikedCount,
          songImages: doc.songsImagesCount
        }
        // fix the number of comments to incllude replies as well
        try {
          const commentCount = await sourceDb.collection('comments').countDocuments({
            'reference.collection': 'artists',
            'reference.id': parseInt(doc._id, 10)
          })
          if (commentCount) {
            newDoc.comments = commentCount
          }
        } catch (e) {
          console.log(e);
        }
        await targetDb.collection(TARGET_COLLECTION).insertOne(newDoc)
      }

      // end
      console.timeEnd(TARGET_COLLECTION)
    }

    // copy and transform blogposts
    async function blogpostsETL() {
      // start
      const TARGET_COLLECTION = 'blogposts'
      console.time(TARGET_COLLECTION)
      console.log(`start ${TARGET_COLLECTION}`)
      try {
        await targetDb.collection(TARGET_COLLECTION).drop()
      } catch (e) {
        console.log(e);
      }

      // etl
      const blogpostsCursor = await sourceDb.collection('blogposts').find()
      while(await blogpostsCursor.hasNext()) {
        const doc = await blogpostsCursor.next()
        doc._id = doc._id.toString()
        try {
          doc.commentsCount = await sourceDb.collection('comments').countDocuments({
            'reference.collection': 'blogposts',
            'reference.id': parseInt(doc._id, 10)
          })
        } catch (e) {
          console.log(e);
        }
        await targetDb.collection(TARGET_COLLECTION).insertOne(doc)
      }

      // end
      console.timeEnd(TARGET_COLLECTION)
    }

    // copy and transform countries
    async function countriesETL() {
      // start
      const TARGET_COLLECTION = 'countries'
      console.time(TARGET_COLLECTION)
      console.log(`start ${TARGET_COLLECTION}`)
      try {
        await targetDb.collection(TARGET_COLLECTION).drop()
      } catch (e) {
        console.log(e);
      }

      // etl
      const countryCursor = await sourceDb.collection('countries').find()
      while(await countryCursor.hasNext()) {
        const doc = await countryCursor.next()
        await targetDb.collection(TARGET_COLLECTION).insertOne(doc)
      }

      // end
      console.timeEnd(TARGET_COLLECTION)
    }

    // copy and transform users
    async function usersETL() {
      // start
      const TARGET_COLLECTION = 'users'
      console.time(TARGET_COLLECTION)
      console.log(`start ${TARGET_COLLECTION}`)
      try {
        await targetDb.collection(TARGET_COLLECTION).drop()
      } catch (e) {
        console.log(e);
      }

      // indexes
      await targetDb.collection(TARGET_COLLECTION).createIndex( { "emails": 1 }, { background: true } )
      await targetDb.collection(TARGET_COLLECTION).createIndex( { "profiles.provider": 1, "profiles.providerId": 1 }, { unique: true, sparse: true, background: true } )

      // etl
      const userCursor = await sourceDb.collection('users').find({ $or: [{ _id: { $gte: 1, $lte: 15 } }, { _id: { $gte: 50455, $lte: 50505 } }]})
      while(await userCursor.hasNext()) {
        const doc = await userCursor.next()
        doc._id = doc._id.toString()
        delete doc.recentlyPlayed
        await targetDb.collection(TARGET_COLLECTION).insertOne(doc)
      }

      // end
      console.timeEnd(TARGET_COLLECTION)
    }

    // copy and transform comments
    async function commentsETL() {
      // start
      const TARGET_COLLECTION = 'comments'
      console.time(TARGET_COLLECTION)
      console.log(`start ${TARGET_COLLECTION}`)
      try {
        await targetDb.collection(TARGET_COLLECTION).drop()
      } catch (e) {
        console.log(e);
      }
      try {
        await targetDb.collection('usercomments').drop()
      } catch (e) {
        console.log(e);
      }

      // indexes
      await targetDb.collection(TARGET_COLLECTION).createIndex( { "reference.id": 1, "parent": 1, "reference.collection": 1 }, { background: true } )
      await targetDb.collection('usercomments').createIndex( { "_id.comment": 1 }, { background: true } )

      // etl
      const commentCursor = await sourceDb.collection('comments').find({ _id: { $gte: 1, $lte: 150 } })
      while(await commentCursor.hasNext()) {
        const doc = await commentCursor.next()
        doc._id = doc._id.toString()
        doc.reference.id = doc.reference.id.toString()
        if (doc.parent) {
          doc.parent = doc.parent.toString()
        }
        for (let i = 0; doc.children && i < doc.children.length; i++) {
          doc.children[i] = doc.children[i].toString()
        }
        doc.user = doc.user.toString()
        if (doc.likers) {
          doc.likeCount = doc.likers.length
          for (let i = 0; i < doc.likers.length; i++) {
            const liker = {
              _id: {
                user: doc.likers[i].toString(),
                comment: doc._id
              },
              like: true
            }
            await targetDb.collection('usercomments').insertOne(liker)
          }
        }
        delete doc.likers
        await targetDb.collection(TARGET_COLLECTION).insertOne(doc)
      }

      // end
      console.timeEnd(TARGET_COLLECTION)
    }

    // copy and transform songs
    async function songsETL() {
      // start
      const TARGET_COLLECTION = 'songs'
      console.time(TARGET_COLLECTION)
      console.log(`start ${TARGET_COLLECTION}`)
      try {
        await targetDb.collection(TARGET_COLLECTION).drop()
      } catch (e) {
        console.log(e);
      }

      // indexes
      await targetDb.collection(TARGET_COLLECTION).createIndex( { "artist": 1 }, { background: true } )

      // etl
      const songCursor = await sourceDb.collection('songs').find({ $or: [{ _id: { $gte: 1, $lte: 50 } }, { _id: { $gte: 150000, $lte: 150005 } }]})
      while(await songCursor.hasNext()) {
        const doc = await songCursor.next()
        doc._id = doc._id.toString()
        doc.artist = doc.artist.toString()
        doc.uploader = doc.uploader.toString()
        if (doc.images) {
          doc.imagesList = []
          for (let i = 0; i < doc.images.length; i++) {
            doc.imagesList[i] = doc.images[i].toString()
          }
          delete doc.images
        }
        if (doc.lyrics) {
          doc.lyrics = doc.lyrics.toString()
        }
        delete doc.videos
        delete doc.vidoesCount
        try {
          doc.commentsCount = await sourceDb.collection('comments').countDocuments({
            'reference.collection': 'songs',
            'reference.id': parseInt(doc._id, 10)
          })
        } catch (e) {
          console.log(e);
        }
        await targetDb.collection(TARGET_COLLECTION).insertOne(doc)
      }

      // end
      console.timeEnd(TARGET_COLLECTION)
    }

    // copy and transform songimages
    async function songimagesETL() {
      // start
      const TARGET_COLLECTION = 'songimages'
      console.time(TARGET_COLLECTION)
      console.log(`start ${TARGET_COLLECTION}`)
      try {
        await targetDb.collection(TARGET_COLLECTION).drop()
      } catch (e) {
        console.log(e);
      }

      // indexes
      await targetDb.collection(TARGET_COLLECTION).createIndex( { "song": 1 }, { background: true } )

      // etl
      const songimageCursor = await sourceDb.collection('songimages').find({ $or: [{ _id: { $gte: 1, $lte: 200 } }]})
      while(await songimageCursor.hasNext()) {
        const doc = await songimageCursor.next()
        doc._id = doc._id.toString()
        doc.song = doc.song.toString()
        doc.addedBy = doc.addedBy.toString()
        for (let i = 0; doc.likers && i < doc.likers.length; i++) {
          doc.likers[i] = doc.likers[i].toString()
        }
        for (let i = 0; doc.dislikers && i < doc.dislikers.length; i++) {
          doc.dislikers[i] = doc.dislikers[i].toString()
        }
        await targetDb.collection(TARGET_COLLECTION).insertOne(doc)
      }

      // end
      console.timeEnd(TARGET_COLLECTION)
    }

    // copy and transform songlyrics
    async function songlyricsETL() {
      // start
      const TARGET_COLLECTION = 'songlyrics'
      console.time(TARGET_COLLECTION)
      console.log(`start ${TARGET_COLLECTION}`)
      try {
        await targetDb.collection(TARGET_COLLECTION).drop()
      } catch (e) {
        console.log(e);
      }

      // indexes
      await targetDb.collection(TARGET_COLLECTION).createIndex( { "song": 1 }, { background: true } )

      // etl
      const lyricsCursor = await sourceDb.collection('songlyrics').find({ $or: [{ _id: { $gte: 1, $lte: 10 } }]})
      while(await lyricsCursor.hasNext()) {
        const doc = await lyricsCursor.next()
        doc._id = doc._id.toString()
        doc.song = doc.song.toString()
        doc.addedBy = doc.addedBy.toString()
        await targetDb.collection(TARGET_COLLECTION).insertOne(doc)
      }

      // end
      console.timeEnd(TARGET_COLLECTION)
    }

    // copy and transform userartists
    async function userartistsETL() {
      // start
      const TARGET_COLLECTION = 'userartists'
      console.time(TARGET_COLLECTION)
      console.log(`start ${TARGET_COLLECTION}`)
      try {
        await targetDb.collection(TARGET_COLLECTION).drop()
      } catch (e) {
        console.log(e);
      }

      // indexes
      await targetDb.collection(TARGET_COLLECTION).createIndex( { "_id.artist": 1 }, { background: true } )
      await targetDb.collection(TARGET_COLLECTION).createIndex( { "_id.user": 1 }, { background: true } )

      // etl
      const userartistsCursor = await sourceDb.collection('userartist').find()
      while(await userartistsCursor.hasNext()) {
        const doc = await userartistsCursor.next()
        const newDoc = {
          _id: {
            user: doc.user.toString(),
            artist: doc.artist.toString()
          },
          like: doc.liked,
          plays: doc.plays
        }
        await targetDb.collection(TARGET_COLLECTION).insertOne(newDoc)
      }

      // end
      console.timeEnd(TARGET_COLLECTION)
    }

    let artists = artistsETL()
    let blogposts = blogpostsETL()
    let countries = countriesETL()
    let users = usersETL()
    let comments = commentsETL()
    let songs = songsETL()
    let songimages = songimagesETL()
    let songlyrics = songlyricsETL()
    let userartists = userartistsETL()

    // run all in parallel
    await artists +
    await blogposts +
    await countries +
    await users +
    await comments +
    await songs +
    await songimages +
    await songlyrics +
    await userartists

    console.timeEnd('all')
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
