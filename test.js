const mix = async () => {
  return new Promise((resolve, reject) => {
    throw new Error('xxx')

    reject(new Error('reject'))
  })
}

const main = async () => {
  // try {
  //   await mix()
  // } catch (err) {
  //   console.log(err)
  // }

  // mix().catch(err => {
  //   console.log(err)
  // })
  mix()
}



main()
