import pydicom


def source_image_sequence(dataset, x):
    """
    Add a Source Image Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the Source Image Sequence will be added.
        x (list): A list containing dictionaries representing the source image sequence data. The expected structure of the dictionaries is:
                  x[0]["00082112"].value[0]["0040A170"].value[0]["00080100"].value
                  x[0]["00082112"].value[0]["0040A170"].value[0]["00080102"].value
                  x[0]["00082112"].value[0]["0040A170"].value[0]["00080104"].value
                  x[0]["00082112"].value[0]["00081150"].value
                  x[0]["00082112"].value[0]["00081155"].value

    This function constructs a PurposeOfReferenceCodeSequence and a SourceImageSequence from the provided data and adds them to the specified DICOM dataset.

    Returns:
        None
    """
    purpose_of_reference_code_seq = pydicom.Sequence()
    purpose_of_reference_code_item = pydicom.Dataset()
    purpose_of_reference_code_item.CodeValue = (
        x[0]["00082112"].value[0]["0040A170"].value[0]["00080100"].value
    )
    purpose_of_reference_code_item.CodingSchemeDesignator = (
        x[0]["00082112"].value[0]["0040A170"].value[0]["00080102"].value
    )
    purpose_of_reference_code_item.CodeMeaning = (
        x[0]["00082112"].value[0]["0040A170"].value[0]["00080104"].value
    )
    purpose_of_reference_code_seq.append(purpose_of_reference_code_item)

    source_image_seq = pydicom.Sequence()
    dataset.SourceImageSequence = source_image_seq
    source_image_item = pydicom.Dataset()
    source_image_item.ReferencedSOPClassUID = (
        x[0]["00082112"].value[0]["00081150"].value
    )
    source_image_item.ReferencedSOPInstanceUID = (
        x[0]["00082112"].value[0]["00081155"].value
    )
    source_image_item.PurposeOfReferenceCodeSequence = purpose_of_reference_code_seq

    source_image_seq.append(source_image_item)

    dataset.SourceImageSequence = source_image_seq


def derivation_algorithm_sequence(dataset, x):
    """
    Add a Derivation Algorithm Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the Derivation Algorithm Sequence will be added.
        x (list): A list containing dictionaries representing the derivation algorithm sequence data. The expected structure of the dictionaries is:
                  x[0]["00221612"].value[0]["0066002F"].value[0]["00080100"].value
                  x[0]["00221612"].value[0]["0066002F"].value[0]["00080102"].value
                  x[0]["00221612"].value[0]["0066002F"].value[0]["00080104"].value
                  x[0]["00221612"].value[0]["00660036"].value
                  x[0]["00221612"].value[0]["00660031"].value

    This function constructs an AlgorithmFamilyCodeSequence and a DerivationAlgorithmSequence from the provided data and adds them to the specified DICOM dataset.

    Returns:
        None
    """
    algorithm_family_code_seq = pydicom.Sequence()
    algorithm_family_code_item = pydicom.Dataset()
    algorithm_family_code_item.CodeValue = (
        x[0]["00221612"].value[0]["0066002F"].value[0]["00080100"].value
    )
    algorithm_family_code_item.CodingSchemeDesignator = (
        x[0]["00221612"].value[0]["0066002F"].value[0]["00080102"].value
    )
    algorithm_family_code_item.CodeMeaning = (
        x[0]["00221612"].value[0]["0066002F"].value[0]["00080104"].value
    )
    algorithm_family_code_seq.append(algorithm_family_code_item)

    derivation_algorithm_seq = pydicom.Sequence()

    derivation_algorithm_item = pydicom.Dataset()
    derivation_algorithm_item.AlgorithmName = (
        x[0]["00221612"].value[0]["00660036"].value
    )
    derivation_algorithm_item.AlgorithmVersion = (
        x[0]["00221612"].value[0]["00660031"].value
    )

    derivation_algorithm_item.AlgorithmFamilyCodeSequence = algorithm_family_code_seq
    derivation_algorithm_seq.append(derivation_algorithm_item)

    dataset.DerivationAlgorithmSequence = derivation_algorithm_seq


def enface_volume_descriptor_sequence(dataset, x, seg):
    """
    Add an En Face Volume Descriptor Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the En Face Volume Descriptor Sequence will be added.
        x (list): A list containing dictionaries representing the en face volume descriptor data. The expected structure of the dictionaries is:
                  x[0]["00221620"].value[i]["0062000F"].value[0]["00080100"].value
                  x[0]["00221620"].value[i]["0062000F"].value[0]["00080102"].value
                  x[0]["00221620"].value[i]["0062000F"].value[0]["00080104"].value
                  x[0]["00221620"].value[i]["00221658"].value
                  x[0]["00221620"].value[i]["0066002C"].value
        seg (list): A list containing dictionaries representing segmentation information. The expected structure is:
                    seg[0]["00080018"].value

    This function constructs a SegmentedPropertyTypeCodeSequence and a ReferencedSegmentationSequence from the provided data,
    and adds them to an En Face Volume Descriptor Sequence in the specified DICOM dataset.

    The function handles cases where the repeat value (length of x[0]["00221620"].value) is either 1 or 2. For 1, it sets the EnFaceVolumeDescriptorScope
    to "ENTIRE". For 2, it uses a mapping to set the scope to "ANTERIOR" or "POSTERIOR".

    Returns:
        None
    """

    enface_volume_descriptor_seq = pydicom.Sequence()
    repeat = len(x[0]["00221620"].value)
    a = 0
    value_mapping = {1: "ANTERIOR", 2: "POSTERIOR"}

    if repeat == 1:

        for i in range(repeat):

            segmented_property_type_code_seq = pydicom.Sequence()
            segmented_property_type_code_item = pydicom.Dataset()
            segmented_property_type_code_item.CodeValue = (
                x[0]["00221620"].value[i]["0062000F"].value[0]["00080100"].value
            )
            segmented_property_type_code_item.CodingSchemeDesignator = (
                x[0]["00221620"].value[i]["0062000F"].value[0]["00080102"].value
            )
            segmented_property_type_code_item.CodeMeaning = (
                x[0]["00221620"].value[i]["0062000F"].value[0]["00080104"].value
            )
            segmented_property_type_code_seq.append(segmented_property_type_code_item)

            referenced_segmentation_seq = pydicom.Sequence()
            referenced_segmentation_item = pydicom.Dataset()
            referenced_segmentation_item.ReferencedSOPClassUID = (
                "1.2.840.10008.5.1.4.xxxxx.1"
            )
            referenced_segmentation_item.ReferencedSOPInstanceUID = seg[0][
                "00080018"
            ].value
            referenced_segmentation_item.ReferencedSegmentNumber = (
                x[0]["00221620"].value[i]["0066002C"].value
            )
            referenced_segmentation_item.SegmentedPropertyTypeCodeSequence = (
                segmented_property_type_code_seq
            )
            referenced_segmentation_seq.append(referenced_segmentation_item)

            enface_volume_descriptor_item = pydicom.Dataset()
            enface_volume_descriptor_item.EnFaceVolumeDescriptorScope = "ENTIRE"
            enface_volume_descriptor_item.ReferencedSegmentationSequence = (
                referenced_segmentation_seq
            )
            enface_volume_descriptor_item.SurfaceOffset = (
                x[0]["00221620"].value[i]["00221658"].value
            )
            enface_volume_descriptor_seq.append(enface_volume_descriptor_item)

        dataset.EnFaceVolumeDescriptorSequence = enface_volume_descriptor_seq

    elif repeat == 2:

        for i in range(repeat):

            a = a + 1

            segmented_property_type_code_seq = pydicom.Sequence()
            segmented_property_type_code_item = pydicom.Dataset()
            segmented_property_type_code_item.CodeValue = (
                x[0]["00221620"].value[i]["0062000F"].value[0]["00080100"].value
            )
            segmented_property_type_code_item.CodingSchemeDesignator = (
                x[0]["00221620"].value[i]["0062000F"].value[0]["00080102"].value
            )
            segmented_property_type_code_item.CodeMeaning = (
                x[0]["00221620"].value[i]["0062000F"].value[0]["00080104"].value
            )
            segmented_property_type_code_seq.append(segmented_property_type_code_item)

            referenced_segmentation_seq = pydicom.Sequence()
            referenced_segmentation_item = pydicom.Dataset()
            referenced_segmentation_item.ReferencedSOPClassUID = (
                "1.2.840.10008.5.1.4.xxxxx.1"
            )
            referenced_segmentation_item.ReferencedSOPInstanceUID = seg[0][
                "00080018"
            ].value
            referenced_segmentation_item.ReferencedSegmentNumber = (
                x[0]["00221620"].value[i]["0066002C"].value
            )
            referenced_segmentation_item.SegmentedPropertyTypeCodeSequence = (
                segmented_property_type_code_seq
            )
            referenced_segmentation_seq.append(referenced_segmentation_item)

            enface_volume_descriptor_item = pydicom.Dataset()
            enface_volume_descriptor_item.EnFaceVolumeDescriptorScope = (
                value_mapping.get(a, "UNKNOWN")
            )
            enface_volume_descriptor_item.ReferencedSegmentationSequence = (
                referenced_segmentation_seq
            )
            enface_volume_descriptor_item.SurfaceOffset = (
                x[0]["00221620"].value[i]["00221658"].value
            )
            enface_volume_descriptor_seq.append(enface_volume_descriptor_item)

        dataset.EnFaceVolumeDescriptorSequence = enface_volume_descriptor_seq

    else:
        "wrong item number"


def referenced_series_sequence(dataset, enface, seg, vol, opt, op):
    """
    Add a Referenced Series Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the Referenced Series Sequence will be added.
        enface (list): A list of dictionaries representing enface data.
        seg (list): A list of dictionaries representing segmentation data.
        vol (list): A list of dictionaries representing volume data.
        opt (list): A list of dictionaries representing optical data.
        op (list): A list of dictionaries representing operational data.

    This function constructs a ReferencedSeriesSequence from the provided data and adds it to the specified DICOM dataset.
    It processes each of the provided lists (op, opt, vol, seg) and creates ReferencedInstanceSequence for each item in the lists.
    If the ReferencedSOPClassUID is "1.2.840.10008.5.1.4.1.1.66.5", it is replaced with "1.2.840.10008.5.1.4.xxxxx.1".

    Returns:
        None
    """

    list = [op, opt, vol, seg]

    referenced_series_seq = pydicom.Sequence()

    for i in list:
        referenced_instance_seq = pydicom.Sequence()

        referenced_instance_item = pydicom.Dataset()
        referenced_instance_item.ReferencedSOPClassUID = i[0]["00080016"].value
        referenced_instance_item.ReferencedSOPInstanceUID = i[0]["00080018"].value

        if (
            referenced_instance_item.ReferencedSOPClassUID
            == "1.2.840.10008.5.1.4.1.1.66.5"
        ):
            referenced_instance_item.ReferencedSOPClassUID = (
                "1.2.840.10008.5.1.4.xxxxx.1"
            )

        referenced_instance_seq.append(referenced_instance_item)

        referenced_series_item = pydicom.Dataset()
        referenced_series_item.SeriesInstanceUID = i[0]["0020000E"].value
        referenced_series_item.StudyInstanceUID = i[0]["0020000D"].value
        referenced_series_item.ReferencedInstanceSequence = referenced_instance_seq
        referenced_series_seq.append(referenced_series_item)

    dataset.ReferencedSeriesSequence = referenced_series_seq


def ophthalmic_image_type_code_sequence(dataset, x):
    """
    Add an Ophthalmic Image Type Code Sequence to a DICOM dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the Ophthalmic Image Type Code Sequence will be added.
        x (list): A list containing dictionaries representing the ophthalmic image type data. The expected structure is:
                  x[0]["00221615"].value[0]["00080104"].value[0]

    This function maps the ophthalmic image type from the provided data to a corresponding code value and adds the code
    to an Ophthalmic Image Type Code Sequence in the specified DICOM dataset.

    The mapping of code values is as follows:
        - "Superficial Retina Vasculature Flow" -> "128265"
        - "Deep retina vasculature flow" -> "128269"
        - "Choriocapillaris vasculature flow" -> "128273"
        - "Outer retina vasculature flow" -> "128271"

    If the provided image type is found in the mapping, the corresponding code value is added to the sequence.
    The CodingSchemeDesignator is set to "DCM" and the CodeMeaning is set to the provided image type with proper capitalization.

    Returns:
        None
    """
    ophthalmic_image_type_code_seq = pydicom.Sequence()
    ophthalmic_image_type_code_item = pydicom.Dataset()
    code_mappings = {
        "Superficial Retina Vasculature Flow": "128265",
        "Deep retina vasculature flow": "128269",
        "Choriocapillaris vasculature flow": "128273",
        "Outer retina vasculature flow": "128271",
    }

    code_value = code_mappings.get(x[0]["00221615"].value[0]["00080104"].value[0])
    if code_value:
        ophthalmic_image_type_code_item.CodeValue = code_value
        ophthalmic_image_type_code_item.CodingSchemeDesignator = "DCM"
        ophthalmic_image_type_code_item.CodeMeaning = (
            x[0]["00221615"].value[0]["00080104"].value[0].lower().capitalize()
        )
        ophthalmic_image_type_code_seq.append(ophthalmic_image_type_code_item)
        dataset.OphthalmicImageTypeCodeSequence = ophthalmic_image_type_code_seq
