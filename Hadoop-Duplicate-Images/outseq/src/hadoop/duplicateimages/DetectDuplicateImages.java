package hadoop.duplicateimages;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.color.ColorSpace;
import java.awt.color.ICC_ColorSpace;
import java.awt.color.ICC_Profile;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;

import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.commons.imaging.ImageReadException;
import org.apache.commons.imaging.common.bytesource.ByteSource;
import org.apache.commons.imaging.common.bytesource.ByteSourceArray;
import org.apache.commons.imaging.formats.jpeg.JpegImageParser;
import org.apache.commons.imaging.formats.jpeg.decoder.JpegDecoder;
import org.apache.commons.imaging.formats.jpeg.segments.UnknownSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.sanselan.Sanselan;

public class DetectDuplicateImages {

	public static final int COLOR_TYPE_RGB = 1;
	public static final int COLOR_TYPE_CMYK = 2;
	public static final int COLOR_TYPE_YCCK = 3;
	private static int colorType = COLOR_TYPE_RGB;
	private static boolean hasAdobeMarker = false;

	public static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable() {
			super(Text.class);
		}

		public TextArrayWritable(String[] strings) {
			super(Text.class);
			Text[] texts = new Text[strings.length];
			for (int i = 0; i < strings.length; i++) {
				texts[i] = new Text(strings[i]);
			}
			set(texts);
		}

		@Override
		public String toString() {
			return "[path file image:] " + this.toStrings()[0];
		}
	}

	// Mapper class
	public static class ImageAhashMapper extends Mapper<Text, BytesWritable, Text, ArrayWritable> {

		final static int WIDTH = 32, HEIGHT = 32;

		// Map function
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			try {
				String hash = avhash(value.getBytes()); // byte[] value
				String[] arr_str = new String[] { key.toString(), hash };

				System.out.println(hash);
				context.write(new Text(hash.substring(0, 4)), new TextArrayWritable(arr_str));

			} catch (InterruptedException e) {

			}
			// System.out.println(pair.getKey()+"----"+pair.getValue());
		}

		// Reducer class
		public static class ImageDupsReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

			// Reduce function
			public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
					throws IOException, InterruptedException {

				// System.out.println("[with KEY: ]"+key.toString());
				TextArrayWritable first = new TextArrayWritable();

				// for(TextArrayWritable tem: values){
				// System.out.println("path = "+tem.toStrings()[0]);
				// System.out.println("hash = "+tem.toStrings()[1]);
				// System.out.println("======================================");

				// }

				for (TextArrayWritable tem : values) {
					first = tem;
					break;
				}
				for (TextArrayWritable last : values) {
					if (first.toStrings()[0] != last.toStrings()[0]) {
						String h1 = first.toStrings()[1];
						String h2 = last.toStrings()[1];
						if (h1 != null && h2 != null) {
							int dist = hamming(h1, h2);
							int percentSimilar = (64 - dist) * 100 / 64;

							if (percentSimilar >= 90)
								context.write(key, last);

						} else {
							System.out.println("Chuoi Rong");
						}
					}
				}
				// System.out.println("path = "+first.toStrings()[0]);
				// System.out.println("hash = "+first.toStrings()[1]);
				// System.out.println("======================================");
				context.write(key, first);
				// System.out.println("Dem = "+count);
			}

			private static int hamming(String h1, String h2) {
				int h = 0;
				long d = new BigInteger(h1).xor(new BigInteger(h2)).longValue();

				while (d != 0) {
					h += 1;
					d &= d - 1;
				}
				return h;
			}
		}

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "DetectImageDuplicates");
			job.setJarByClass(DetectDuplicateImages.class);
			job.setMapperClass(ImageAhashMapper.class);
			job.setCombinerClass(ImageDupsReducer.class);
			job.setReducerClass(ImageDupsReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(TextArrayWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

		public static String avhash(byte[] bimg) throws IOException, InterruptedException {
			BigInteger result = new BigInteger("0");
			try {
				BufferedImage inputimage = readImage(bimg);

				BufferedImage outimage = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_BYTE_GRAY);

				Graphics2D g2d = outimage.createGraphics();
				g2d.drawImage(inputimage, 0, 0, WIDTH, HEIGHT, null);
				RenderingHints rh = new RenderingHints(RenderingHints.KEY_ANTIALIASING,
						RenderingHints.VALUE_ANTIALIAS_ON);// 14829788209170334145
															// 14829788209170334145
				g2d.setRenderingHints(rh);
				g2d.dispose();

				int sumpixelvalue = 0, c = 0;
				int[] arrpixelvalue = new int[WIDTH * HEIGHT];
				outimage.getRGB(0, 0, WIDTH, HEIGHT, arrpixelvalue, 0, WIDTH); // = new int[WIDTH];
				for (int row = 0; row < HEIGHT; row++)
					for (int col = 0; col < WIDTH; col++) {
						arrpixelvalue[c] = outimage.getRaster().getSample(row, col, 0);
						sumpixelvalue += arrpixelvalue[c];
						// System.out.print(arrpixelvalue[c]+", ");
						c++;

					}
				// System.out.println("\n");

				double avg = sumpixelvalue / (WIDTH * HEIGHT * 1.0);

				BigInteger bigvalue[] = new BigInteger[arrpixelvalue.length];
				for (int i = 0; i < WIDTH * HEIGHT; i++) {
					bigvalue[i] = arrpixelvalue[i] < avg ? BigInteger.ZERO : BigInteger.ONE;

				}
				// System.out.println("\n");

				for (int i = 0; i < WIDTH * HEIGHT; i++)
					result = result.or(bigvalue[i].shiftLeft(i));
			} catch (ImageReadException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (org.apache.sanselan.ImageReadException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// System.out.println("hash: ("+path+") = "+result.toString());
			return result.toString();
		}

		public static BufferedImage readImage(byte[] bimg)
				throws IOException, ImageReadException, org.apache.sanselan.ImageReadException {
			colorType = COLOR_TYPE_RGB;
			hasAdobeMarker = false;

			InputStream is = new ByteArrayInputStream(bimg);

			ImageInputStream stream = ImageIO.createImageInputStream(is);

			Iterator<ImageReader> iter = ImageIO.getImageReaders(stream);
			while (iter.hasNext()) {
				ImageReader reader = iter.next();
				reader.setInput(stream);

				BufferedImage image;
				ICC_Profile profile = null;
				try {
					image = reader.read(0);
				} catch (IIOException e) {
					colorType = COLOR_TYPE_CMYK;
					checkAdobeMarker(bimg);
					profile = Sanselan.getICCProfile(bimg);
					WritableRaster raster = (WritableRaster) reader.readRaster(0, null);
					if (colorType == COLOR_TYPE_YCCK)
						convertYcckToCmyk(raster);
					if (hasAdobeMarker)
						convertInvertedColors(raster);
					image = convertCmykToRgb(raster, profile);
				}

				return image;
			}

			return null;
		}

		public static void checkAdobeMarker(byte[] bimg) throws IOException, ImageReadException {
			JpegImageParser parser = new JpegImageParser();
			ByteSource byteSource = new ByteSourceArray(bimg);
			@SuppressWarnings("rawtypes")
			ArrayList segments = (ArrayList) parser.readSegments(byteSource, new int[] { 0xffee }, true);
			if (segments != null && segments.size() >= 1) {
				UnknownSegment app14Segment = (UnknownSegment) segments.get(0);
				byte[] data = app14Segment.bytes;
				if (data.length >= 12 && data[0] == 'A' && data[1] == 'd' && data[2] == 'o' && data[3] == 'b'
						&& data[4] == 'e') {
					hasAdobeMarker = true;
					int transform = app14Segment.bytes[11] & 0xff;
					if (transform == 2)
						colorType = COLOR_TYPE_YCCK;
				}
			}
		}

		public static void convertYcckToCmyk(WritableRaster raster) {
			int height = raster.getHeight();
			int width = raster.getWidth();
			int stride = width * 4;
			int[] pixelRow = new int[stride];
			for (int h = 0; h < height; h++) {
				raster.getPixels(0, h, width, 1, pixelRow);

				for (int x = 0; x < stride; x += 4) {
					int y = pixelRow[x];
					int cb = pixelRow[x + 1];
					int cr = pixelRow[x + 2];

					int c = (int) (y + 1.402 * cr - 178.956);
					int m = (int) (y - 0.34414 * cb - 0.71414 * cr + 135.95984);
					y = (int) (y + 1.772 * cb - 226.316);

					if (c < 0)
						c = 0;
					else if (c > 255)
						c = 255;
					if (m < 0)
						m = 0;
					else if (m > 255)
						m = 255;
					if (y < 0)
						y = 0;
					else if (y > 255)
						y = 255;

					pixelRow[x] = 255 - c;
					pixelRow[x + 1] = 255 - m;
					pixelRow[x + 2] = 255 - y;
				}

				raster.setPixels(0, h, width, 1, pixelRow);
			}
		}

		public static void convertInvertedColors(WritableRaster raster) {
			int height = raster.getHeight();
			int width = raster.getWidth();
			int stride = width * 4;
			int[] pixelRow = new int[stride];
			for (int h = 0; h < height; h++) {
				raster.getPixels(0, h, width, 1, pixelRow);
				for (int x = 0; x < stride; x++)
					pixelRow[x] = 255 - pixelRow[x];
				raster.setPixels(0, h, width, 1, pixelRow);
			}
		}

		public static BufferedImage convertCmykToRgb(Raster cmykRaster, ICC_Profile cmykProfile) throws IOException {
			if (cmykProfile == null)
				cmykProfile = ICC_Profile
						.getInstance(JpegDecoder.class.getResourceAsStream("/ISOcoated_v2_300_eci.icc"));
			ICC_ColorSpace cmykCS = new ICC_ColorSpace(cmykProfile);
			BufferedImage rgbImage = new BufferedImage(cmykRaster.getWidth(), cmykRaster.getHeight(),
					BufferedImage.TYPE_INT_RGB);
			WritableRaster rgbRaster = rgbImage.getRaster();
			ColorSpace rgbCS = rgbImage.getColorModel().getColorSpace();
			ColorConvertOp cmykToRgb = new ColorConvertOp(cmykCS, rgbCS, null);
			cmykToRgb.filter(cmykRaster, rgbRaster);
			return rgbImage;
		}
	}

}
