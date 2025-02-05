import { Context, Hono } from 'hono';
import { HTTPException } from 'hono/http-exception';

type Env = {
	BUCKET: R2Bucket;
	AUTH_KEY: string;
};

const getQueryString = (c: Context, name: string): string => {
	const val = c.req.query(name);
	if (val == null) {
		throw new HTTPException(400, { message: `Missing ${name}` });
	}
	return val;
};
const getQueryNumber = (c: Context, name: string): number => {
	const val = getQueryString(c, name);
	const n = parseInt(val, 10);
	if (isNaN(n)) {
		throw new HTTPException(400, { message: `Invalid ${name}` });
	}
	return n;
};
const getPathString = (c: Context, name: string): string => {
	const val = c.req.param(name);
	if (val == null) {
		throw new HTTPException(400, { message: `Missing ${name}` });
	}
	return val;
};
const getPathNumber = (c: Context, name: string): number => {
	const val = getPathString(c, name);
	const n = parseInt(val, 10);
	if (isNaN(n)) {
		throw new HTTPException(400, { message: `Invalid ${name}` });
	}
	return n;
};

const app = new Hono<{ Bindings: Env }>();
app.onError((err, c) => {
	console.error(err);
	if (err instanceof HTTPException) {
		return c.json({ error: err.message }, { status: err.status });
	}
	return c.json({ error: 'Internal Server Error' }, { status: 500 });
});
app.use(async (c, next) => {
	if (c.env.BUCKET == null) {
		return Response.json({ error: 'Missing BUCKET binding' }, { status: 500 });
	}
	if (c.env.AUTH_KEY == null) {
		return Response.json({ error: 'Missing AUTH_KEY secret' }, { status: 500 });
	}
	return next();
});
app.post('/uploads/create', async (c) => {
	const key = getQueryString(c, 'key');
	const mpu = await c.env.BUCKET.createMultipartUpload(key);
	return c.json({
		key: mpu.key,
		uploadId: mpu.uploadId,
	});
});
app.post('/uploads/upload-part', async (c) => {
	if (c.req.raw.body == null) {
		throw new HTTPException(400, { message: 'No body' });
	}
	const key = getQueryString(c, 'key');
	const uploadId = getQueryString(c, 'uploadId');
	const partNumber = getQueryNumber(c, 'partNumber');
	const mpu = c.env.BUCKET.resumeMultipartUpload(key, uploadId);
	const part = await mpu.uploadPart(partNumber, c.req.raw.body);
	return c.json({
		partNumber: part.partNumber,
		etag: part.etag,
	});
});
app.post('/uploads/complete', async (c) => {
	const key = getQueryString(c, 'key');
	const uploadId = getQueryString(c, 'uploadId');
	const parts = await c.req.json();
	if (!Array.isArray(parts)) {
		throw new HTTPException(400, { message: 'Invalid parts' });
	}
	if (!parts.every((part) => typeof part.partNumber === 'number' && typeof part.etag === 'string')) {
		throw new HTTPException(400, { message: 'Invalid parts' });
	}
	if (parts.length === 0) {
		throw new HTTPException(400, { message: 'No parts' });
	}
	const mpu = c.env.BUCKET.resumeMultipartUpload(key, uploadId);
	await mpu.complete(parts);
	return c.json({ key });
});

app.get('/objects', async (c) => {
	const prefix = c.req.query('prefix');
	const listed = await c.env.BUCKET.list({
		prefix,
		limit: 1000,
	});
	const objects = listed.objects.map((v) => ({
		key: v.key,
		size: v.size,
		etag: v.etag,
	}));
	return c.json(objects);
});
app.get('/objects/:key', async (c) => {
	const key = getPathString(c, 'key');
	const object = await c.env.BUCKET.get(key);
	if (object == null) {
		throw new HTTPException(404, { message: 'Not found' });
	}
	const h = new Headers();
	object.writeHttpMetadata(h);
	return new Response(object.body, {
		headers: h,
	});
});
app.get('/objects/:key/stats', async (c) => {
	const key = getPathString(c, 'key');
	const object = await c.env.BUCKET.head(key);
	if (object == null) {
		throw new HTTPException(404, { message: 'Not found' });
	}
	return c.json({
		size: object.size,
		etag: object.etag,
	});
});
app.delete('/objects/:key', async (c) => {
	const key = getPathString(c, 'key');
	await c.env.BUCKET.delete(key);
	return c.newResponse(null, { status: 204 });
});

export default app;
