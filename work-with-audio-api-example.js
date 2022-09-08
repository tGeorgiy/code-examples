const recognizeAudio = async (ownerId, fileId, filePath) => {
  const client = new ApiClient(ACCESS_TOKEN);
  const jobOptions = {
    language: 'ru',
    delete_after_seconds: 360,
    callback_url: 'https://*********/api/recognize/callback',
  };

  try {
    const buff = getSignedUrl(filePath);

    // Submit an audio link to API
    const response = await client.submitJobUrl(buff, jobOptions);

    // Submit an audio file to Rev.ai
    const data = { status: 'Draft', recognizedTextPath: response.id };
    await updateFileRecord(ownerId, fileId, data);
  } catch (error) {
    throw new AppError(error);
  }
  return { message: 'Audio was sent.' };
};

// After the work is done, this function is called by the callback url
const recognizeAudioCallback = async (audioId) => {
  const client = new ApiClient(ACCESS_TOKEN);
  try {
    const transcript = await client.getTranscriptText(audioId);
    const recognizedAudioDetails = await client.getJobDetails(audioId);
    const { dataValues: { id, ownerId } } = await getAudioFileRecordByApiId(audioId);
    if (recognizedAudioDetails.status === 'failed') {
      await file(ownerId, id, { status: 'Failed' });
      throw new AppError('Recognizing faild');
    }
    const recognizedFilePath = await createRecognizedTextFile(transcript);
    const wordsCount = transcript
      .replace(/[.,/#!|$%+^&*;?:{}=\-_`~()0123456789\n+]/g, '')
      .replace(/\s{2,}/, ' ')
      .split(' ')
      .length;
    const data = {
      length: recognizedAudioDetails.duration_seconds,
      recognizedTextPath: recognizedFilePath,
      words: wordsCount,
      status: 'Completed',
    };
    await updateFileRecord(ownerId, id, data);
  } catch (error) {
    throw new AppError(error);
  }
  // Deleting Job from Api
  await client.deleteJob(audioId);
};
