// Packaged into ../handler.zip. Substrate records a Lambda function's code and
// never runs it, so this handler exists only to make the archive a real one.
export const handler = async (event) => ({ statusCode: 200, body: JSON.stringify(event) });
