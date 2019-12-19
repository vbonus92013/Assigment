package hadoop.duplicateimages;

import java.util.*;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.ArrayWritable;

import java.awt.image.Raster;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.net.URI;
import javax.activation.MimetypesFileTypeMap;
import java.awt.Graphics2D;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.RenderingHints;
import java.math.BigInteger;

public class DetectDuplicateImages {

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
	public static class ImageAhashMapper extends Mapper<Object, Text, Text, ArrayWritable> {

		final static int WIDTHHASH = 8, HEIGHTHASH = 8;
		final static int WIDTHKEY = 4, HEIGHTKEY = 4;

		// Map function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				Configuration conf = new Configuration();
				String path = value.toString();
				FileSystem fs = FileSystem.get(URI.create(path), conf);

				MimetypesFileTypeMap mimetypesFileTypeMap = new MimetypesFileTypeMap();
				mimetypesFileTypeMap.addMimeTypes("image png tif jpg jpeg bmp webp hdr bat bpg svg");

				String mimetype = mimetypesFileTypeMap.getContentType(path);
				String type = mimetype.split("/")[0];
				if (type.equals("image")) {
					// System.out.println("It's an image");
					FSDataInputStream fsDataInputStream = fs.open(new Path(path));
					BufferedImage inputimage = null;
					try {
						ImageInputStream input = ImageIO.createImageInputStream(fsDataInputStream);

						try {
							inputimage = ImageIO.read(input);
						} catch (Exception e) {
							System.out.println("here: " + e);
						}

					} catch (Exception e) {

						// Catch image encoding with CMYK

						System.out.println(e);
						Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("JPEG");
						ImageReader reader = null;
						// Find a suitable ImageReader
						while (readers.hasNext()) {
							reader = (ImageReader) readers.next();
							if (reader.canReadRaster()) {
								break;
							}
						}
						// Stream the image file (the original CMYK image)
						ImageInputStream input = ImageIO.createImageInputStream(fsDataInputStream);
						reader.setInput(input);
						// Read the image raster
						Raster raster = reader.readRaster(0, null);
						// Create a new RGB image
						inputimage = new BufferedImage(raster.getWidth(), raster.getHeight(),
								BufferedImage.TYPE_INT_ARGB_PRE);
						// Fill the new image with the old raster
						inputimage.getRaster().setRect(raster);
					}
					if (inputimage != null) {
						String hash = avhash(inputimage, WIDTHHASH, HEIGHTHASH); // 
						String mykey = avhash(inputimage, WIDTHKEY, HEIGHTKEY);
						String[] arr_str = new String[] { value.toString(), hash };
						context.write(new Text(mykey), new TextArrayWritable(arr_str));
					}
				} else {
					System.out.println("It's NOT an image");
				}

			} catch (

			InterruptedException e) {
			}
			// System.out.println(pair.getKey()+"----"+pair.getValue());
		}

		public static String avhash(BufferedImage inputimage, int width, int height)
				throws IOException, InterruptedException {
			BigInteger result = new BigInteger("0");
			// File f = new File(path);
			try {

				BufferedImage outimage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);

				Graphics2D g2d = outimage.createGraphics();
				g2d.drawImage(inputimage, 0, 0, width, height, null);
				RenderingHints rh = new RenderingHints(RenderingHints.KEY_ANTIALIASING,
						RenderingHints.VALUE_ANTIALIAS_ON);// 14829788209170334145
															// 14829788209170334145
				g2d.setRenderingHints(rh);
				g2d.dispose();

				int sumpixelvalue = 0, c = 0;
				int[] arrpixelvalue = new int[width * height];
				outimage.getRGB(0, 0, width, height, arrpixelvalue, 0, width); // = new int[WIDTH];
				for (int row = 0; row < height; row++)
					for (int col = 0; col < width; col++) {
						arrpixelvalue[c] = outimage.getRaster().getSample(row, col, 0);
						sumpixelvalue += arrpixelvalue[c];
						// System.out.print(arrpixelvalue[c]+", ");
						c++;

					}
				// System.out.println("\n");

				double avg = sumpixelvalue / (width * height * 1.0);

				BigInteger bigvalue[] = new BigInteger[arrpixelvalue.length];
				for (int i = 0; i < width * height; i++) {
					bigvalue[i] = arrpixelvalue[i] < avg ? BigInteger.ZERO : BigInteger.ONE;

				}
				// System.out.println("\n");

				for (int i = 0; i < width * height; i++)
					result = result.or(bigvalue[i].shiftLeft(i));
			} catch (Exception e) {

			}
			return result.toString();
		}
	}

	// Reducer class
	public static class ImageDupsCombiner extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

		// Reduce function
		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			// TextArrayWritable first = new TextArrayWritable();

			ArrayList<TextArrayWritable> listItem = new ArrayList<>();
			System.out.println("[with KEY: ]" + key.toString());
			for (TextArrayWritable tem : values) {
				// first = tem;
				// break;
				// System.out.println("[item: ]" + tem.toStrings()[0]);
				TextArrayWritable newitem = new TextArrayWritable(
						new String[] { tem.toStrings()[0], tem.toStrings()[1] });
				listItem.add(newitem);
			}

			for (int i = 0; i < listItem.size(); i++) {
				for (int j = i + 1; j < listItem.size(); j++) {

					String h1 = listItem.get(i).toStrings()[1];
					String h2 = listItem.get(j).toStrings()[1];
					if (h1 != null && h2 != null) {
						int dist = hamming(h1, h2);
						int percentSimilar = (64 - dist) * 100 / 64;
						if (percentSimilar >= 90) {
							System.out.println((listItem.get(i).toStrings()[0] + " similar with: "
									+ listItem.get(j).toStrings()[0]));
							context.write(key, listItem.get(j));
						}
						// count++;
					} else {

						System.out.println("Chuoi Rong");
					}

				}
				// System.out.println(listItem.get(i).toStrings()[0]);
			}
			System.out.println("======================================");
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

	public static class ImageDupsReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

		// Reduce function
		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			TextArrayWritable dup = values.iterator().next();

			context.write(key, dup);

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DetectImageDuplicates");
		job.setJarByClass(DetectDuplicateImages.class);
		job.setMapperClass(ImageAhashMapper.class);
		job.setCombinerClass(ImageDupsCombiner.class);
		job.setReducerClass(ImageDupsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextArrayWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
