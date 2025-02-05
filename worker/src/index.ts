type Env = {
	BUCKET: R2Bucket;
	AUTH_KEY: string;
};

const checkMethod = (req: Request, method: string) => {
	if (req.method !== method) {
		throw new Response('Method not allowed', { status: 405 });
	}
};
const getUrlParam = (req: Request, name: string): string => {
	const url = new URL(req.url);
	const value = url.searchParams.get(name);
	if (typeof value !== 'string') {
		throw new Response(`Invalid or missing ${name}`, { status: 400 });
	}
	return value;
};
const getUrlParamNumber = (req: Request, name: string): number => {
	const value = getUrlParam(req, name);
	const n = parseInt(value, 10);
	if (isNaN(n)) {
		throw new Response(`Invalid ${name}`, { status: 400 });
	}
	return n;
};

const handleRequest = async (request: Request, env: Env) => {
	const auth = request.headers.get('Authorization');
	if (typeof auth !== 'string' || auth !== env.AUTH_KEY) {
		throw Response.json({ error: 'Unauthorized' }, { status: 401 });
	}

	const url = new URL(request.url);
	const firstPath = url.pathname.split('/').at(1);
	if (firstPath == null) {
		throw Response.json({ error: 'Not found' }, { status: 404 });
	}

	switch (firstPath) {
		case 'start-upload': {
			checkMethod(request, 'POST');

			const key = getUrlParam(request, 'key');
			const mpu = await env.BUCKET.createMultipartUpload(key);
			return Response.json({
				key: mpu.key,
				uploadId: mpu.uploadId,
			});
		}
		case 'upload-part': {
			checkMethod(request, 'POST');
			if (request.body == null) {
				throw Response.json({ error: 'No body' }, { status: 400 });
			}

			const key = getUrlParam(request, 'key');
			const uploadId = getUrlParam(request, 'uploadId');
			const partNumber = getUrlParamNumber(request, 'partNumber');
			const mpu = env.BUCKET.resumeMultipartUpload(key, uploadId);
			const part = await mpu.uploadPart(partNumber, request.body);
			return Response.json({
				partNumber: part.partNumber,
				etag: part.etag,
			});
		}
		case 'complete-upload': {
			checkMethod(request, 'POST');

			const key = getUrlParam(request, 'key');
			const uploadId = getUrlParam(request, 'uploadId');

			// Validate body.
			const parts = await request.json();
			if (!Array.isArray(parts)) {
				return Response.json({ error: 'Invalid parts' }, { status: 400 });
			}
			if (!parts.every((part) => typeof part.partNumber === 'number' && typeof part.etag === 'string')) {
				return Response.json({ error: 'Invalid parts' }, { status: 400 });
			}
			if (parts.length === 0) {
				return Response.json({ error: 'No parts' }, { status: 400 });
			}

			const mpu = env.BUCKET.resumeMultipartUpload(key, uploadId);
			await mpu.complete(parts);
			return Response.json({ key });
		}
		case 'download': {
			checkMethod(request, 'GET');
			const key = getUrlParam(request, 'key');

			const object = await env.BUCKET.get(key);
			if (object == null) {
				return Response.json({ error: 'Not found' }, { status: 404 });
			}

			const h = new Headers();
			object.writeHttpMetadata(h);
			return new Response(object.body, {
				headers: h,
			});
		}
		case 'stats': {
			checkMethod(request, 'GET');
			const key = getUrlParam(request, 'key');
			const object = await env.BUCKET.head(key);
			if (object == null) {
				return Response.json({ error: 'Not found' }, { status: 404 });
			}
			return Response.json({
				size: object.size,
				etag: object.etag,
			});
		}
		default: {
			throw Response.json({ error: 'Not found' }, { status: 404 });
		}
	}
};

export default {
	async fetch(request, env, ctx): Promise<Response> {
		try {
			return await handleRequest(request, env);
		} catch (e) {
			if (e instanceof Response) {
				return e;
			}
			console.error(e);
			return Response.json({ error: 'Internal server error' }, { status: 500 });
		}
	},
} satisfies ExportedHandler<Env>;
